using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Server.Commands;
using Sparrow.Json;

namespace Raven.Client.Http
{
    public class ClusterRequestExecutor : RequestExecutor
    {
        private readonly SemaphoreSlim _clusterTopologySemaphore = new SemaphoreSlim(1, 1);

        protected ClusterRequestExecutor(string apiKey) : base(null, apiKey)
        {
        }

        [Obsolete("Not supported", error: true)]
        public new static ClusterRequestExecutor Create(string[] urls, string databaseName, string apiKey)
        {
            throw new NotSupportedException();
        }

        [Obsolete("Not supported", error: true)]
        public new static ClusterRequestExecutor CreateForSingleNode(string url, string databaseName, string apiKey)
        {
            throw new NotSupportedException();
        }

        public static ClusterRequestExecutor CreateForSingleNode(string url, string apiKey)
        {
            {
                var executor = new ClusterRequestExecutor(apiKey)
                {
                    _nodeSelector = new NodeSelector(new Topology
                    {
                        Etag = -1,
                        Nodes = new List<ServerNode>
                        {
                            new ServerNode
                            {
                                Url = url
                            }
                        }
                    }),
                    TopologyEtag = -2,
                    _withoutTopology = true
                };
                return executor;
            }
        }

        public static ClusterRequestExecutor Create(string[] urls, string apiKey)
        {
            var executor = new ClusterRequestExecutor(apiKey);
            executor._firstTopologyUpdate = executor.FirstTopologyUpdate(urls);
            return executor;
        }
        
        
        protected override async Task PerformHealthCheck(ServerNode serverNode, JsonOperationContext context)
        {
            await ExecuteAsync(serverNode, context, new GetTcpInfoCommand("health-check"), shouldRetry: false).ConfigureAwait(false);
        }

        public override async Task<bool> UpdateTopologyAsync(ServerNode node, int timeout)
        {
            if (_disposed)
                return false;
            var lockTaken = await _clusterTopologySemaphore.WaitAsync(timeout).ConfigureAwait(false);
            if (lockTaken == false)
                return false;
            try
            {
                if (_disposed)
                    return false;

                using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new GetClusterTopologyCommand();

                    await ExecuteAsync(node, context, command, shouldRetry: false).ConfigureAwait(false);

                    var serverHash = ServerHash.GetServerHash(node.Url);
                    ClusterTopologyLocalCache.TrySavingTopologyToLocalCache(serverHash, command.Result, context);

                    var results = command.Result;

                    var newTopology = new Topology
                    {
                        Nodes = new List<ServerNode>(
                            from member in results.Topology.Members
                            select new ServerNode
                            {
                                Url = member.Value,
                                ClusterTag = member.Key
                            }
                        )
                    };
                    if (_nodeSelector == null)
                    {
                        _nodeSelector = new NodeSelector(newTopology);
                    }
                    else if (_nodeSelector.OnUpdateTopology(newTopology))
                    {
                        DisposeAllFailedNodesTimers();
                    }

                }
            }
            finally
            {
                _clusterTopologySemaphore.Release();
            }
            return true;
        }

        public override void Dispose()
        {
            _clusterTopologySemaphore.Wait();
            base.Dispose();
        }

        protected override bool TryLoadFromCache(string url, JsonOperationContext context)
        {
            var serverHash = ServerHash.GetServerHash(url);
            var cachedTopology = ClusterTopologyLocalCache.TryLoadClusterTopologyFromLocalCache(serverHash, context);

            if (cachedTopology == null)
                return false;

            _nodeSelector = new NodeSelector(new Topology
            {
                Nodes = new List<ServerNode>(
                    from member in cachedTopology.Topology.Members
                    select new ServerNode
                    {
                        Url = member.Value,
                        ClusterTag = member.Key
                    }
                )
            });
            return true;
        }
    }
}