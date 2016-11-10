//-----------------------------------------------------------------------
// <copyright file="HiLoKeyGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Util;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Data;
using Raven.Client.Documents.Commands;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Json.Linq;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Async
{
    /// <summary>
    /// Generate hilo numbers against a RavenDB document
    /// </summary>
    public class AsyncHiLoKeyGenerator : HiLoKeyGeneratorBase
    {
        private SpinLock generatorLock = new SpinLock(enableThreadOwnerTracking: false); // Using a spin lock rather than Monitor.Enter, because it's not reentrant

        /// <summary>
        /// Initializes a new instance of the <see cref="Document.HiLoKeyGenerator"/> class.
        /// </summary>
        public AsyncHiLoKeyGenerator(string tag, long capacity)
            : base(tag, capacity)
        {
        }

        /// <summary>
        /// Generates the document key.
        /// </summary>
        /// <param name="convention">The convention.</param>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public Task<string> GenerateDocumentKeyAsync(DocumentConvention convention, object entity, JsonOperationContext context, RequestExecuter requestExecuter)
        {
            return NextIdAsync(context, requestExecuter).ContinueWith(task => GetDocumentKeyFromId(convention, task.Result));
        }

        ///<summary>
        /// Create the next id (numeric)
        ///</summary>
        public Task<long> NextIdAsync(JsonOperationContext context, RequestExecuter requestExecuter)
        {
            var myRange = Range; // thread safe copy
            long incrementedCurrent = Interlocked.Increment(ref myRange.Current);
            if (incrementedCurrent <= myRange.Max)
            {
                return CompletedTask.With(incrementedCurrent);
            }

            bool lockTaken = false;
            try
            {
                generatorLock.Enter(ref lockTaken);
                if (Range != myRange)
                {
                    // Lock was contended, and the max has already been changed. Just get a new id as usual.
                    generatorLock.Exit();
                    return NextIdAsync(context, requestExecuter);
                }
                // Get a new max, and use the current value.
                return GetNextRangeAsync(context, requestExecuter)
                    .ContinueWith(task =>
                    {
                        try
                        {
                            Range = task.Result;
                        }
                        finally
                        {
                            generatorLock.Exit();
                        }

                        return NextIdAsync(context, requestExecuter);
                    }).Unwrap();
            }
            catch
            {
                // We only unlock in exceptional cases (and not in a finally clause) because non exceptional cases will either have already
                // unlocked or will have started a task that will unlock in the future.
                if (lockTaken)
                    generatorLock.Exit();
                throw;
            }
        }

        private Task<RangeValue> GetNextRangeAsync(JsonOperationContext context, RequestExecuter requestExecuter)
        {
            ModifyCapacityIfRequired();

            return GetNextMaxAsyncInner(context, requestExecuter);
        }

        private async Task<RangeValue> GetNextMaxAsyncInner(JsonOperationContext context, RequestExecuter requestExecuter)
        {
            var minNextMax = Range.Max;

            //using (databaseCommands.ForceReadFromMaster())
                while (true)
                {
                    try
                    {
                        ConflictException ce = null;
                        InMemoryDocumentSessionOperations.DocumentInfo documentInfo;
                    try
                        {
                        documentInfo = await GetDocumentAsync(context, requestExecuter, new[] { HiLoDocumentKey, RavenKeyServerPrefix });
                        }
                        catch (ConflictException e)
                        {
                            ce = e;
                        documentInfo = null;
                        }
                        if (ce != null)
                            return await HandleConflictsAsync(context, requestExecuter, ce, minNextMax).ConfigureAwait(false);

                        long min, max;
                        if (documentInfo.Document == null)
                        {
                            min = minNextMax + 1;
                            max = minNextMax + capacity;
                            documentInfo.Id = HiLoDocumentKey;
                            documentInfo.Document = context.ReadObject(new DynamicJsonValue() { ["Max"] = max }, HiLoDocumentKey);
                            documentInfo.ETag = 0;  
                        }
                        else
                        {
                            var oldMax = GetMaxFromDocument(documentInfo.Document, minNextMax);
                            min = oldMax + 1;
                            max = oldMax + capacity;

                            documentInfo.Document.Modifications["Max"] = max;
                        }

                        await PutDocumentAsync(context, requestExecuter, documentInfo);
                        return new RangeValue(min, max);
                    }
                    catch (ConcurrencyException)
                    {
                        //expected & ignored, will retry this
                    }
                }
        }

        private async Task<RangeValue> HandleConflictsAsync(JsonOperationContext context, RequestExecuter requestExecuter, ConflictException e, long minNextMax)
        {
            // resolving the conflict by selecting the highest number
            long highestMax = -1;
            if (e.ConflictedVersionIds.Length == 0)
                throw new InvalidOperationException("Got conflict exception, but no conflicted versions", e);
            foreach (var conflictedVersionId in e.ConflictedVersionIds)
            {
                var doc = await GetDocumentAsync(context, requestExecuter, new[] { conflictedVersionId });
                highestMax = Math.Max(highestMax, GetMaxFromDocument(doc.Document, minNextMax));
            }

            var documentInfo = new InMemoryDocumentSessionOperations.DocumentInfo()
            {
                ETag = e.Etag,
                Id = HiLoDocumentKey,
                Document = context.ReadObject(new DynamicJsonValue() { ["Max"] = highestMax }, HiLoDocumentKey)
            };
            await PutDocumentAsync(context, requestExecuter, documentInfo);
            
            return await GetNextRangeAsync(context, requestExecuter).ConfigureAwait(false);
        }

        private async Task PutDocumentAsync(JsonOperationContext context, RequestExecuter requestExecuter, InMemoryDocumentSessionOperations.DocumentInfo documentInfo)
        {
            //Myble to add metadata to document -EFRAT
            var command = new PutDocumentCommand()
            {
                Id = HiLoDocumentKey,
                Etag = documentInfo.ETag,
                Document = documentInfo.Document,
                Context = context
            };

            await requestExecuter.ExecuteAsync(command, context);
        }

        private async Task<InMemoryDocumentSessionOperations.DocumentInfo> GetDocumentAsync(JsonOperationContext context, RequestExecuter requestExecuter, string[] ids)
        {
            var command = new GetDocumentCommand
            {
                Ids = ids
            };

            await requestExecuter.ExecuteAsync(command, context);

            if (command.Result.Results.Length == 2 && command.Result.Results[1] != null)
            {
                object value;
                ((BlittableJsonReaderObject)(command.Result.Results[1])).TryGetMember("ServerPrefix", out value);
                lastServerPrefix = value.ToString();
            }
            else
            {
                lastServerPrefix = string.Empty;
            }

            if (command.Result.Results.Length == 0 || command.Result.Results[0] == null)
                return null;

            var document = (BlittableJsonReaderObject)command.Result.Results[0];
            object metadata;
            document.TryGetMember(Constants.Metadata.Key, out metadata);

            object etag;
            document.TryGetMember(Constants.Metadata.Etag, out etag);

            //TODO - Efrat , we need this???
            /*foreach (var key in jsonDocument.Metadata.Keys.Where(x => x.StartsWith("@")).ToArray())
            {
                jsonDocument.Metadata.Remove(key);
            }*/

            return new InMemoryDocumentSessionOperations.DocumentInfo()
            {
                Document = document,
                ETag = etag as long?,
                Metadata = metadata as BlittableJsonReaderObject
            };

          
        }
    }
}
