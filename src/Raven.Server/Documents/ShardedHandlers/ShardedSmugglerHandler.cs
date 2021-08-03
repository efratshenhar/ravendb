using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Net.Http.Headers;
using Nito.AsyncEx;
using Raven.Client;
using Raven.Client.Documents.Linq.Indexing;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Json;
using Raven.Client.Util;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.Documents.Sharding;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using DatabaseSmuggler = Raven.Server.Smuggler.Documents.DatabaseSmuggler;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedSmugglerHandler : ShardedRequestHandler
    {

        [RavenShardedAction("/databases/*/smuggler/export", "POST")]
        public async Task PostExport()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                BlittableJsonReaderObject blittableJson = null;
                DatabaseSmugglerOptionsServerSide options;

                //TODO - EFRAT - From where we are getting startEtag and  startRaftIndex to export? do we need it
                var stream = TryGetRequestFromStream("DownloadOptions") ?? RequestBodyStream();

                using (context.GetMemoryBuffer(out var buffer))
                {
                    var firstRead = await stream.ReadAsync(buffer.Memory.Memory);
                    buffer.Used = 0;
                    buffer.Valid = firstRead;
                    if (firstRead != 0)
                    {
                        blittableJson = await context.ParseToMemoryAsync(stream, "DownloadOptions", BlittableJsonDocumentBuilder.UsageMode.None, buffer);
                        options = JsonDeserializationServer.DatabaseSmugglerOptions(blittableJson);
                    }
                    else
                    {
                        // no content, we'll use defaults
                        options = new DatabaseSmugglerOptionsServerSide();
                    }

                    var fileName = options.FileName;
                    if (string.IsNullOrEmpty(fileName))
                    {
                        fileName = $"Dump of {ShardedContext.DatabaseName} {SystemTime.UtcNow.ToString("yyyy-MM-dd HH-mm", CultureInfo.InvariantCulture)}";
                    }

                    var contentDisposition = "attachment; filename=" + Uri.EscapeDataString(fileName) + ".ravendbdump";
                    HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
                    HttpContext.Response.Headers["Content-Type"] = "application/octet-stream";

                    try
                    {
                        var operationId = GetLongQueryString("operationId", false) ?? ServerStore.Operations.GetNextOperationId();
                        var token = CreateOperationToken();

                        await ServerStore.Operations.AddOperation(
                            null,
                            "Export database: " + ShardedContext.DatabaseName,
                            Operations.Operations.OperationType.DatabaseExport,
                            onProgress => ExportShardedDatabaseInternalAsync(options, fileName, onProgress, blittableJson, context, token),
                            operationId, token: token);
                    }
                    catch (Exception)
                    {
                        HttpContext.Abort();
                    }
                }

            }
        }

        public async Task<IOperationResult> ExportShardedDatabaseInternalAsync(
            DatabaseSmugglerOptionsServerSide options,
            string fileName,
            Action<IOperationProgress> onProgress,
            BlittableJsonReaderObject blittableJson,
            JsonOperationContext context,
            OperationCancelToken token)
        {

            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            

            try
            {
                var first = true;
                using(var outputStream = GetOutputStream(ResponseBodyStream(), options))
                using (var fileStream = new GZipStream(outputStream, CompressionMode.Compress))
                {
                    for (int i = 0; i < ShardedContext.ShardCount; i++)
                    {
                        var cmd = new ShardedExportCommand(this, async stream =>
                        {
                            try
                            {
                                var tempStream = new byte[8193];
                                using (var gzipStream = new GZipStream(GetInputStream(stream, options), CompressionMode.Decompress))
                                {
                                    if (first == false)
                                    {
                                        var t = new byte[18];
                                        await gzipStream.ReadAsync(t, new CancellationToken());
                                    }

                                    var result = await FillBuffer(gzipStream, tempStream, isFirst: true);

                                    while (result.HasMore)
                                    {
                                        await fileStream.WriteAsync(new ReadOnlyMemory<byte>(tempStream, 0, result.Read), new CancellationToken());
                                        result = await FillBuffer(gzipStream, tempStream, isFirst: false);
                                    }

                                    await fileStream.WriteAsync(tempStream, 0, result.Read - 1);
                                }
                            }
                            catch (Exception e)
                            {
                                //if (Logger.IsOperationsEnabled)
                                //     Logger.Operations("Could not save export file.", e);

                                tcs.TrySetException(e);

                                if (e is UnauthorizedAccessException || e is DirectoryNotFoundException || e is IOException)
                                    throw new InvalidOperationException($"Cannot export to selected path {fileName}, please ensure you selected proper filename.", e);

                                throw new InvalidOperationException($"Could not save export file {fileName}.", e);
                            }
                        }, tcs, blittableJson);

                        await ShardedContext.RequestExecutors[i].ExecuteAsync(cmd, context);
                        if (i == ShardedContext.ShardCount - 1)
                        {
                            await fileStream.WriteAsync(Encoding.UTF8.GetBytes("}"));
                        }
                        else
                        {
                            if (first)
                            {
                                blittableJson.Modifications = new DynamicJsonValue(blittableJson)
                                {
                                    ["OperateOnTypes"] = options.OperateOnTypes & ~DatabaseItemType.DatabaseRecord
                                };
                                using (var old = blittableJson)
                                {
                                    blittableJson = context.ReadObject(blittableJson, "convert/entityToBlittable");
                                }

                                first = false;
                            }
                        }



                        


                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
            

            return null;

        }

        private static async Task<(bool HasMore, int Read)> FillBuffer(GZipStream gzipStream, byte[] tempStream, bool isFirst)
        {
            var hasMore = false;
            var start = isFirst ? 0 : 1;
            
            if (isFirst == false)
            {
                tempStream[0] = tempStream[8192];
            }

            var num = await gzipStream.ReadAsync(new Memory<byte>(tempStream, start, 8192 - start), new CancellationToken());
            if (await gzipStream.ReadAsync(new Memory<byte>(tempStream, 8192, 1), new CancellationToken()) == 1)
            {
                num++;
                hasMore = true;
            }

            return (hasMore, num);
        }

        private Stream GetOutputStream(Stream fileStream, DatabaseSmugglerOptionsServerSide options)
        {
            if (options.EncryptionKey == null)
                return fileStream;

            var key = options?.EncryptionKey;
            return new EncryptingXChaCha20Poly1305Stream(fileStream,
                Convert.FromBase64String(key));
        }

        private Stream GetInputStream(Stream fileStream, DatabaseSmugglerOptionsServerSide options)
        {
            if (options.EncryptionKey != null)
                return new DecryptingXChaCha20Oly1305Stream(fileStream, Convert.FromBase64String(options.EncryptionKey));

            return fileStream;
        }

        [RavenShardedAction("/databases/*/smuggler/import", "POST")]
        public async Task PostImportAsync()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                if (HttpContext.Request.HasFormContentType == false)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest; // Bad request
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue { ["Type"] = "Error", ["Error"] = "This endpoint requires form content type" });
                        return;
                    }
                }

                var operationId = GetLongQueryString("operationId", false) ?? Server.ServerStore.Operations.GetNextOperationId();
                var token = CreateOperationToken();

                var result = new SmugglerResult();
                BlittableJsonReaderObject blittableJson = null;
                await Server.ServerStore.Operations.AddOperation(null, "Import to: ",
                    Operations.Operations.OperationType.DatabaseImport,
                    onProgress =>
                    {
                        return Task.Run(async () =>
                        {
                            try
                            {
                                var boundary = MultipartRequestHelper.GetBoundary(
                                    MediaTypeHeaderValue.Parse(HttpContext.Request.ContentType),
                                    MultipartRequestHelper.MultipartBoundaryLengthLimit);
                                var reader = new MultipartReader(boundary, HttpContext.Request.Body);
                                DatabaseSmugglerOptionsServerSide options = null;

                                while (true)
                                {
                                    var section = await reader.ReadNextSectionAsync().ConfigureAwait(false);
                                    if (section == null)
                                        break;

                                    if (ContentDispositionHeaderValue.TryParse(section.ContentDisposition, out ContentDispositionHeaderValue contentDisposition) == false)
                                        continue;

                                    if (MultipartRequestHelper.HasFormDataContentDisposition(contentDisposition))
                                    {
                                        var key = HeaderUtilities.RemoveQuotes(contentDisposition.Name);
                                        if (key != Constants.Smuggler.ImportOptions)
                                            continue;


                                        if (section.Headers.ContainsKey("Content-Encoding") && section.Headers["Content-Encoding"] == "gzip")
                                        {
                                            await using (var gzipStream = new GZipStream(section.Body, CompressionMode.Decompress))
                                            {
                                                blittableJson = await context.ReadForMemoryAsync(gzipStream, Constants.Smuggler.ImportOptions);
                                            }
                                        }
                                        else
                                        {
                                            blittableJson = await context.ReadForMemoryAsync(section.Body, Constants.Smuggler.ImportOptions);
                                        }

                                        options = JsonDeserializationServer.DatabaseSmugglerOptions(blittableJson);
                                        continue;
                                    }

                                    if (MultipartRequestHelper.HasFileContentDisposition(contentDisposition) == false)
                                        continue;

                                    var inputStream = GetInputStream(section.Body, options);
                                    var stream = new GZipStream(inputStream, CompressionMode.Decompress);
                                    await DoImportInternalAsync(context, stream, blittableJson, options, result, onProgress, token);
                                }
                            }
                            catch (Exception e)
                            {
                                result.AddError($"Error occurred during import. Exception: {e.Message}");
                                onProgress.Invoke(result.Progress);
                                throw;
                            }

                            return (IOperationResult)result;
                        });
                    }, operationId, token: token).ConfigureAwait(false);

                // await WriteImportResultAsync(context, result, ResponseBodyStream());


            }
        }

        private async Task DoImportInternalAsync(JsonOperationContext context, Stream stream, BlittableJsonReaderObject optionsAsBlittable,DatabaseSmugglerOptionsServerSide options, SmugglerResult result, Action<IOperationProgress> onProgress,
            OperationCancelToken token)
        {
            var contextList = new List<JsonOperationContext>();
            try
            {
                var tasks = new List<Task>();
                for (int i = 0; i < ShardedContext.ShardCount; i++)
                {
                    ContextPool.AllocateOperationContext(out JsonOperationContext jsonOperationContext);
                    contextList.Add(jsonOperationContext);
                }

                var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

                var streamList = new List<Stream>();
                for (int i = 0; i < ShardedContext.ShardCount; i++)
                {
                    streamList.Add(GetOutputStream(new MemoryStream(), options));
                }

                options.OperateOnTypes = options.OperateOnTypes & ~DatabaseItemType.DatabaseRecord;

                // Sharded Database Destination
                // using (ContextPool.AllocateOperationContext(out TransactionOperationContext tContext))
                // using (var source = new StreamSource(stream, context, ShardedContext.DatabaseName))
                // using (tContext.OpenReadTransaction())
                //
                // {
                //     var record = Server.ServerStore.Cluster.ReadDatabase(tContext, ShardedContext.DatabaseName);
                //     var destination = new ShardedDatabaseDestination(streamList, context, tContext, source, record.ShardAllocations);
                //     var smuggler = new DatabaseSmuggler(null, source, destination, new SystemTime(), context, options, result, onProgress, token: token.Token);
                //
                //     await smuggler.ExecuteAsync();
                // }
                var destinationList = new List<ISmugglerDestination>();
                using (ContextPool.AllocateOperationContext(out TransactionOperationContext tContext))
                using (var source = new StreamSource(stream, context, ShardedContext.DatabaseName))
                using (tContext.OpenReadTransaction())

                {
                    var record = Server.ServerStore.Cluster.ReadDatabase(tContext, ShardedContext.DatabaseName);
                    for (int i = 0; i < ShardedContext.ShardCount; i++)
                    {
                        destinationList.Add(new StreamDestination(streamList[i], contextList[i], source));
                    }
                    //var destination = new ShardedDatabaseDestination(streamList, context, tContext, source, record.ShardAllocations);
                    var smuggler = new ShardedDatabaseSmuggler(source, destinationList, context, tContext, record.ShardAllocations, new SystemTime(), options, result, onProgress, token: token.Token);

                    await smuggler.ExecuteAsync();
                }


                for (int i = 0; i < ShardedContext.ShardCount; i++)
                {
                    streamList[i].Position = 0;
                   
                    var multi = new MultipartFormDataContent
                    {
                        {
                            new BlittableJsonContent(async stream2 => await context.WriteAsync(stream2, optionsAsBlittable).ConfigureAwait(false)),
                            Constants.Smuggler.ImportOptions
                        },
                        {new Client.Documents.Smuggler.DatabaseSmuggler.StreamContentWithConfirmation(streamList[i], tcs), "file", "name"}
                    };
                    var cmd = new ShardedImportCommand(this, Headers.None, multi);
                    var task = ShardedContext.RequestExecutors[i].ExecuteAsync(cmd, contextList[i]);
                    tasks.Add(task);
                }

                await tasks.WhenAll();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
            finally
            {
                for (int i = 0; i < ShardedContext.ShardCount; i++)
                {
                    contextList[i].Dispose();
                }
                //File.Delete(tempFileName);
            }
        }
    }
}
