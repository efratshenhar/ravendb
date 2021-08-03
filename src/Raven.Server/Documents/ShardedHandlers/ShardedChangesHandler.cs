using System.Threading.Tasks;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedChangesHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/changes", "GET")]
        public async Task GetChanges()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {

            }
        }

    }
}
