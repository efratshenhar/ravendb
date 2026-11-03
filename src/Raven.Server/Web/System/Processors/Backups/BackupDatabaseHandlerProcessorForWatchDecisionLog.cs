using System;
using System.Diagnostics;
using System.Net.WebSockets;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Backups;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Web.System.Processors.Backups;

internal sealed class BackupDatabaseHandlerProcessorForWatchDecisionLog : AbstractServerHandlerWebSocketProxyProcessor
{
    private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer<BackupDatabaseHandlerProcessorForWatchDecisionLog>();

    private static readonly TimeSpan CheckInterval = TimeSpan.FromSeconds(1);

    private static readonly TimeSpan SummaryInterval = TimeSpan.FromSeconds(5);

    private const int MaxFilterSizeInBytes = 64 * 1024;

    public BackupDatabaseHandlerProcessorForWatchDecisionLog([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override string GetRemoteEndpointUrl() => "/admin/debug/periodic-backup/decision-log/watch";

    protected override async ValueTask HandleCurrentNodeAsync(WebSocket webSocket, OperationCancelToken token)
    {
        var runner = ServerStore.ServerBackupRunner;
        var buffer = new byte[4096];
        var receive = ReceiveFilterAsync(webSocket, buffer, token);
        BackupDecisionLogFilter filter = null;
        long lastEtag = 0;
        var sinceLastSummary = Stopwatch.StartNew();

        try
        {
            while (token.Token.IsCancellationRequested == false)
            {
                if (receive.IsCompleted)
                {
                    BackupDecisionLogFilter receivedFilter;

                    try
                    {
                        receivedFilter = await receive;
                    }
                    catch (BadRequestException e)
                    {
                        await WebSocketProxy.SendErrorAsync(ServerStore, webSocket, e, Logger);

                        receive = ReceiveFilterAsync(webSocket, buffer, token);
                        continue;
                    }

                    if (receivedFilter == null)
                    {
                        await webSocket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, null, token.Token);
                        return;
                    }

                    filter = receivedFilter;
                    lastEtag = runner.LastDecisionEtag;

                    await SendAsync(webSocket, "Snapshot", runner.GetDecisionLogDetails(filter), token);
                    sinceLastSummary.Restart();

                    receive = ReceiveFilterAsync(webSocket, buffer, token);
                    continue;
                }

                if (filter != null)
                {
                    var currentEtag = runner.LastDecisionEtag;
                    var includeSummary = sinceLastSummary.Elapsed >= SummaryInterval;

                    if (currentEtag != lastEtag || includeSummary)
                    {
                        var details = runner.GetDecisionLogDetails(filter, afterEtag: lastEtag, includeSummary);
                        lastEtag = currentEtag;

                        await SendAsync(webSocket, "Update", details, token);

                        if (includeSummary)
                            sinceLastSummary.Restart();
                    }
                }

                await Task.WhenAny(receive, Task.Delay(CheckInterval, token.Token));
            }
        }
        finally
        {
            if (receive.IsCompleted == false)
                _ = receive.ContinueWith(static t => _ = t.Exception, TaskContinuationOptions.OnlyOnFaulted);
        }
    }

    private async Task<BackupDecisionLogFilter> ReceiveFilterAsync(WebSocket webSocket, byte[] buffer, OperationCancelToken token)
    {
        await using (var ms = RecyclableMemoryStreamFactory.GetRecyclableStream())
        {
            var tooLarge = false;

            while (true)
            {
                var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), token.Token);
                if (result.MessageType == WebSocketMessageType.Close)
                    return null;

                tooLarge |= ms.Length + result.Count > MaxFilterSizeInBytes;

                if (tooLarge == false)
                    ms.Write(buffer, 0, result.Count);

                if (result.EndOfMessage)
                    break;
            }

            if (tooLarge)
                throw new BadRequestException($"The filter message is larger than {MaxFilterSizeInBytes} bytes.");

            ms.Position = 0;

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                try
                {
                    var json = await context.ReadForMemoryAsync(ms, "backup-decision-log-filter");

                    return BackupDecisionLogFilter.FromJson(json, BackupDatabaseHandlerProcessorForGetDecisionLog.DefaultPageSize, BackupDatabaseHandlerProcessorForGetDecisionLog.MaxPageSize);
                }
                catch (Exception e) when (e is not BadRequestException && e is not OperationCanceledException)
                {
                    throw new BadRequestException($"Could not read the filter: {e.Message}", e);
                }
            }
        }
    }

    private async Task SendAsync(WebSocket webSocket, string type, BackupDecisionLogDetails details, OperationCancelToken token)
    {
        using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var ms = RecyclableMemoryStreamFactory.GetRecyclableStream())
        {
            var json = details.ToJson();
            json["Type"] = type;

            await using (var writer = new AsyncBlittableJsonTextWriter(context, ms))
            {
                context.Write(writer, json);
            }

            ms.TryGetBuffer(out ArraySegment<byte> bytes);
            await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, token.Token);
        }
    }
}
