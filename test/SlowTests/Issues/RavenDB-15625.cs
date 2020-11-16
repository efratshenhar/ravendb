using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15625 : ClusterTestBase
    {
        public RavenDB_15625(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task NotInRehabWithDisabledIndexes()
        {
            var settings = new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "5"
            };
            var leader = await CreateRaftClusterAndGetLeader(3, customSettings:settings);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                var documentDatabase = await Servers[2].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                var definition = new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name", Storage = FieldStorage.No }, });
                string indexName = definition.Name;
                AsyncHelpers.RunSync(() => documentDatabase.IndexStore.CreateIndex(definition, Guid.NewGuid().ToString()));
                WaitForIndexingInTheCluster(store);
                foreach (var server in Servers)
                {
                    documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                    var index = documentDatabase.IndexStore.GetIndex(indexName);
                    index.SetState(IndexState.Disabled);
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                record.Topology.Members.Remove(Servers[2].ServerStore.NodeTag);
                record.Topology.Rehabs.Add(Servers[2].ServerStore.NodeTag);
                await store.Maintenance.Server.SendAsync(new UpdateDatabaseOperation(record, record.Etag));

                var rehabs = await WaitForValueAsync(async () => await GetRehabCount(store, store.Database), 1);
                Assert.Equal(1, rehabs);

                var val = await WaitForValueAsync(async () => await GetMembersCount(store, database), 2);
                Assert.Equal(2, val);

                await Task.Delay(TimeSpan.FromSeconds(10));

                val = await WaitForValueAsync(async () => await GetMembersCount(store, store.Database), 3);
                Assert.Equal(3, val);

                rehabs = await WaitForValueAsync(async () => await GetRehabCount(store, store.Database), 0);
                Assert.Equal(0, rehabs);
            }
        }

        [Fact]
        public async Task StayInRehabWithDisabledIndexes()
        {
            var settings = new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "5"
            };
            var leader = await CreateRaftClusterAndGetLeader(3, customSettings: settings);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                var documentDatabase = await Servers[2].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                var definition = new AutoMapIndexDefinition("Users", new[] { new AutoIndexField { Name = "Name", Storage = FieldStorage.No }, });
                string indexName = definition.Name;
                AsyncHelpers.RunSync(() => documentDatabase.IndexStore.CreateIndex(definition, Guid.NewGuid().ToString()));
                var index = documentDatabase.IndexStore.GetIndex(indexName);
                index.SetState(IndexState.Disabled);

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                record.Topology.Members.Remove(Servers[2].ServerStore.NodeTag);
                record.Topology.Rehabs.Add(Servers[2].ServerStore.NodeTag);
                await store.Maintenance.Server.SendAsync(new UpdateDatabaseOperation(record, record.Etag));

                var rehabs = await WaitForValueAsync(async () => await GetRehabCount(store, store.Database), 1);
                Assert.Equal(1, rehabs);

                var val = await WaitForValueAsync(async () => await GetMembersCount(store, database), 2);
                Assert.Equal(2, val);

                await Task.Delay(TimeSpan.FromSeconds(10));

                val = await WaitForValueAsync(async () => await GetMembersCount(store, store.Database), 2);
                Assert.Equal(2, val);

                rehabs = await WaitForValueAsync(async () => await GetRehabCount(store, store.Database), 1);
                Assert.Equal(1, rehabs);
            }
        }

        private static async Task<int> GetMembersCount(IDocumentStore store, string databaseName)
        {
            var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
            if (res == null)
            {
                return -1;
            }
            return res.Topology.Members.Count;
        }

        private static async Task<int> GetRehabCount(IDocumentStore store, string databaseName)
        {
            var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
            if (res == null)
            {
                return -1;
            }
            return res.Topology.Rehabs.Count;
        }
    }
}
