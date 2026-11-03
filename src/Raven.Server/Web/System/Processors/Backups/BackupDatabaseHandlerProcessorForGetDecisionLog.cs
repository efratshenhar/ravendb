using System;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Backups;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.Backups;

internal sealed class BackupDatabaseHandlerProcessorForGetDecisionLog : AbstractServerHandlerProxyReadProcessor<BlittableJsonReaderObject>
{
    internal const int DefaultPageSize = 512;

    internal const int MaxPageSize = 4096;

    public BackupDatabaseHandlerProcessorForGetDecisionLog([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var filter = GetFilter();

        using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            var details = ServerStore.ServerBackupRunner.GetDecisionLogDetails(filter);

            context.Write(writer, details.ToJson());
        }
    }

    protected override ValueTask<RavenCommand<BlittableJsonReaderObject>> CreateCommandForNodeAsync(string nodeTag, JsonOperationContext context)
    {
        var queryString = QueryString.Create(HttpContext.Request.Query.Where(x => string.Equals(x.Key, Constants.QueryString.NodeTag, StringComparison.OrdinalIgnoreCase) == false));

        return ValueTask.FromResult<RavenCommand<BlittableJsonReaderObject>>(new GetDecisionLogCommand(nodeTag, queryString.ToUriComponent()));
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<BlittableJsonReaderObject> command, JsonOperationContext context, OperationCancelToken token)
    {
        return ServerStore.ClusterRequestExecutor.ExecuteAsync(command, context, token: token.Token);
    }

    private BackupDecisionLogFilter GetFilter()
    {
        return new BackupDecisionLogFilter
        {
            Databases = RequestHandler.GetStringValuesQueryString("database", required: false).ToHashSet(StringComparer.OrdinalIgnoreCase),
            TaskIds = RequestHandler.GetStringValuesQueryString("taskId", required: false).Select(BackupDecisionLogFilter.ParseTaskId).ToHashSet(),
            Categories = RequestHandler.GetStringValuesQueryString("category", required: false).Select(BackupDecisionLogFilter.ParseCategory).ToHashSet(),
            Reason = RequestHandler.GetStringQueryString("reason", required: false),
            From = RequestHandler.GetDateTimeQueryString("from", required: false),
            To = RequestHandler.GetDateTimeQueryString("to", required: false),
            Take = Math.Clamp(RequestHandler.GetIntValueQueryString("take", required: false) ?? DefaultPageSize, 0, MaxPageSize)
        };
    }

    private sealed class GetDecisionLogCommand : RavenCommand<BlittableJsonReaderObject>
    {
        private readonly string _queryString;

        public GetDecisionLogCommand(string nodeTag, string queryString)
        {
            _queryString = queryString;
            SelectedNodeTag = nodeTag;
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/debug/periodic-backup/decision-log{_queryString}";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }
    }
}
