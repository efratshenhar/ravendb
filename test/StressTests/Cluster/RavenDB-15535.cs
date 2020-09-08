using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using FastTests.Utils;
using Nito.AsyncEx;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Cluster
{
    public class RavenDB_15535 : ReplicationTestBase
    {
        public RavenDB_15535(ITestOutputHelper output) : base(output)
        {
        }

        // protected override RavenServer GetNewServer(ServerCreationOptions options = null, [CallerMemberName] string caller = null)
        // {
        //     if (options == null)
        //     {
        //         options = new ServerCreationOptions();
        //     }
        //     if (options.CustomSettings == null)
        //         options.CustomSettings = new Dictionary<string, string>();
        //
        //     options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.OperationTimeout)] = "10";
        //     options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1";
        //     options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.TcpConnectionTimeout)] = "3000";
        //     options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = "50";
        //
        //     return base.GetNewServer(options, caller);
        // }

        public async Task createRevisions(DocumentStore store)
        {
            store.Initialize();
            var r = new Random();
            for (int i = 0; i < 150; i++)
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    await session.StoreAsync(new User { Name = "Toli" }, "user/" + r.Next(1, 100));
                    await session.SaveChangesAsync();
                }
            }
            
            for (int i = 0; i < 100; i++)
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var rand1 = r.Next(1, 100);
                    var rand2 = r.Next(1, 100);
                    await session.StoreAsync(new User { Name = "Toli", Count = i*rand1 }, "user/" + rand1);
                    if (rand1 != rand2)
                        session.Delete("user/" + rand2);
                    await session.SaveChangesAsync();
                }
            }
            for (int i = 0; i < 1000; i++)
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var rand1 = r.Next(1, 100);
                    var rand2 = r.Next(1, 100);
                    var user = await session.LoadAsync<User>("user/" + rand1);
                    if (user != null)
                    {
                        user.Age = r.Next(1, 50);
                        await session.StoreAsync(user);
                    }
                    // if (rand1 != rand2)
                    //     session.Delete("user/" + rand2);
                    await session.SaveChangesAsync();
                }
            }



        }
        [Fact]
        public async Task ClusterTransactionRequestWithRevisions2()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var leader = await CreateRaftClusterAndGetLeader(3, shouldRunInMemory: false, leaderIndex: 0);
            using (var leaderStore = GetDocumentStore(new Options
            {
                DeleteDatabaseOnDispose = false,
                Server = leader,
                ReplicationFactor = 3,
                ModifyDocumentStore = (store) => store.Conventions.DisableTopologyUpdates = true
            }))
            {
                try
                {
                    var index = await RevisionsHelper.SetupRevisions(leader.ServerStore, leaderStore.Database, configuration =>
                    {
                        configuration.Collections["Users"].PurgeOnDelete = false;
                        configuration.Collections["Users"].MinimumRevisionsToKeep = 50;
                    });
                    await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));
                    var t = new List<Task>();
                    foreach (var server in Servers)
                    {
                        Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context);
                        
                            var store = new DocumentStore {Database = leaderStore.Database, Urls = new string[] {server.WebUrl}};
                            
                            t.Add(createRevisions(store));
                    }
                    await Task.WhenAll(t);
                    
                    
                    for (int i = 1; i <= 100; i++)
                    {
                        var cl = new List<int>(3);
                        var bl = new List<int>(3);
                        foreach (var server in Servers)
                        {
                            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                            {
                                
                                using (var store = new DocumentStore
                                {
                                    Database = leaderStore.Database,
                                    Urls = new string[] { server.WebUrl }
                                }.Initialize())
                                using (var session = store.OpenAsyncSession())
                                {
                                    cl.Add(session.Advanced.Revisions.GetForAsync<User>("user/" + i).Result.Count);
                                    var command = new GetRevisionsBinEntryCommand(long.MaxValue, int.MaxValue);
                                    await store.GetRequestExecutor().ExecuteAsync(command, context);
                                    bl.Add(command.Result.Results.Length);
                                }
                            }
                        }

                        if ((cl[0] != cl[1]) || (cl[0] != cl[2]))
                        {
                            WaitForUserToContinueTheTest(leaderStore);
                        }
                        if ((bl[0] != bl[1]) || (bl[0] != bl[2]))
                        {
                            WaitForUserToContinueTheTest(leaderStore);
                        }
                        //Assert.Equal(cl[0], cl[1]);
                        //Assert.Equal(cl[0], cl[2]);
                        //Assert.Equal(bl[0], bl[1]);
                        //Assert.Equal(bl[0], bl[2]);

                    }
                    //WaitForUserToContinueTheTest(leaderStore);

                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
                
                
            }
        }

        [Fact]
        public async Task ClusterTransactionRequestWithRevisions()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var leader = await CreateRaftClusterAndGetLeader(2, shouldRunInMemory: false, leaderIndex: 0);
            using (var leaderStore = GetDocumentStore(new Options
            {
                DeleteDatabaseOnDispose = false,
                Server = leader,
                ReplicationFactor = 2,
                ModifyDocumentStore = (store) => store.Conventions.DisableTopologyUpdates = true
            }))
            {
                try
                {
                    var index = await RevisionsHelper.SetupRevisions(leader.ServerStore, leaderStore.Database, configuration =>
                    {
                        configuration.Collections["Users"].PurgeOnDelete = false;
                        configuration.Collections["Users"].MinimumRevisionsToKeep = 50;
                    });
                    await WaitForRaftIndexToBeAppliedInCluster(index, TimeSpan.FromSeconds(15));
                    leaderStore.Initialize();
                    //var l = Servers[0].GetTcpServerStatus().Listeners;
                    //var database = await GetDocumentDatabaseInstanceFor(leaderStore, leaderStore.Database);
                    var database = Servers[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(leaderStore.Database).Result;
                    var tcpList = database.RunningTcpConnections;
                    
                    //WaitForUserToContinueTheTest(leaderStore);
                    foreach (var tcpClient in tcpList)
                    {
                        // var endPoint = tcpClient.TcpClient.Client.RemoteEndPoint;
                        // tcpClient.TcpClient.
                        // //tcpClient.TcpClient.Client.Disconnect(false);
                        // WaitForUserToContinueTheTest(leaderStore);
                        // //tcpClient.TcpClient.Client.Connect(endPoint);

                    }



                    WaitForUserToContinueTheTest(leaderStore);
                    using (var session = leaderStore.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        await session.StoreAsync(new User { Name = "Toli" }, "user/1" );
                        await session.SaveChangesAsync();
                    }
                    using (var session = leaderStore.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Toli",Age = 10}, "user/1");
                        await session.SaveChangesAsync();
                    }
                    WaitForUserToContinueTheTest(leaderStore);
                }
                
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }


            }
        }
    }
}
