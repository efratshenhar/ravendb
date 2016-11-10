//-----------------------------------------------------------------------
// <copyright file="HiLoKeyGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading;
using Raven.Client.Connection;
using Raven.Client.Data;
using Raven.Client.Document;
using Raven.Client.Documents.Commands;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Json.Linq;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents
{
    /// <summary>
    /// Generate hilo numbers against a RavenDB document
    /// </summary>
    public class HiLoKeyGenerator : HiLoKeyGeneratorBase
    {
        private readonly object generatorLock = new object();

        /// <summary>
        /// Initializes a new instance of the <see cref="HiLoKeyGenerator"/> class.
        /// </summary>
        public HiLoKeyGenerator(string tag, long capacity)
            : base(tag, capacity)
        {
        }

        /// <summary>
        /// Generates the document key.
        /// </summary>
        /// <param name="convention">The convention.</param>
        /// <param name="entity">The entity.</param>
        /// <param name="databaseCommands">Low level database commands.</param>
        /// <returns></returns>
        public string GenerateDocumentKey(JsonOperationContext context, RequestExecuter requestExecuter, DocumentConvention convention, object entity)
        {
            return GetDocumentKeyFromId(convention, NextId(context, requestExecuter));
        }

        ///<summary>
        /// Create the next id (numeric)
        ///</summary>
        public long NextId(JsonOperationContext context, RequestExecuter requestExecuter)
        {
            while (true)
            {
                var myRange = Range; // thread safe copy

                var current = Interlocked.Increment(ref myRange.Current);
                if (current <= myRange.Max)
                    return current;

                lock (generatorLock)
                {
                    if (Range != myRange)
                        // Lock was contended, and the max has already been changed. Just get a new id as usual.
                        continue;

                    Range = GetNextRange(context, requestExecuter);
                }
            }
        }

        private RangeValue GetNextRange(JsonOperationContext context, RequestExecuter requestExecuter)
        {
             /*using (databaseCommands.ForceReadFromMaster())
             {*/
                 ModifyCapacityIfRequired();
                 while (true)
                 {
                     try
                     {
                         var minNextMax = Range.Max;
                    InMemoryDocumentSessionOperations.DocumentInfo documentInfo;

                         try
                         {
                        documentInfo = GetDocument(context, requestExecuter, new[] { HiLoDocumentKey, RavenKeyServerPrefix });
                         }
                         catch (ConflictException e)
                         {
                             // resolving the conflict by selecting the highest number
                             var highestMax = e.ConflictedVersionIds
                                 .Select(conflictedVersionId => GetMaxFromDocument(GetDocument(context, requestExecuter, new[] { conflictedVersionId }).Document, minNextMax))
                                 .Max();

                             PutDocument(context, requestExecuter, new InMemoryDocumentSessionOperations.DocumentInfo()
                             {
                                 ETag = e.Etag,
                                 Id = HiLoDocumentKey,
                                 Document = context.ReadObject(new DynamicJsonValue() {["Max"] = highestMax }, HiLoDocumentKey ),
                             });
                            
                             continue;
                         }

                         long min, max;
                         if (documentInfo.Document == null)
                         {
                             min = minNextMax + 1;
                             max = minNextMax + capacity;

                             documentInfo.Id = HiLoDocumentKey;
                             documentInfo.Document = context.ReadObject(new DynamicJsonValue() {["Max"] = max}, HiLoDocumentKey);
                             documentInfo.ETag = 0;
                         }
                         else
                         {
                            var oldMax = GetMaxFromDocument(documentInfo.Document, minNextMax);
                            min = oldMax + 1;
                            max = oldMax + capacity;

                            documentInfo.Document.Modifications["Max"] = max;
                         }
                         PutDocument(context, requestExecuter, documentInfo);

                         return new RangeValue(min, max);
                     }
                     catch (ConcurrencyException)
                     {
                         // expected, we need to retry
                     }
                 }
             //}
        }

        private void PutDocument(JsonOperationContext context, RequestExecuter requestExecuter, InMemoryDocumentSessionOperations.DocumentInfo documentInfo)
        {
            //Myble to add metadata to document -EFRAT
            var command = new PutDocumentCommand()
            {
                Id = HiLoDocumentKey,
                Etag = documentInfo.ETag,
                Document = documentInfo.Document,
                Context = context
            };

            requestExecuter.Execute(command, context);
        }

        private InMemoryDocumentSessionOperations.DocumentInfo GetDocument(JsonOperationContext context, RequestExecuter requestExecuter, string[] ids)
        {
            var command = new GetDocumentCommand
            {
                Ids = ids 
            };

            requestExecuter.Execute(command, context);

            //var document = (BlittableJsonReaderObject)command.Result.Results[0];

            return HandleGetDocumentResult(command.Result);
        }
    }
}
