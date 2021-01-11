using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Issues;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Tests.Indexes;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15900 : ReplicationTestBase
    {
        public RavenDB_15900(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task SkipEntryFromRaftLogTest()
        {
            var leader = await CreateRaftClusterAndGetLeader(1);
            var database = GetDatabaseName();
            
            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 1, leader.WebUrl, null);
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                var documentDatabase = await leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                using (documentDatabase.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenWriteTransaction())
                {
            
                    Task newTask = Task.Run(async () =>
                    {
                        var cmd = new RachisConsensusTestBase.TestCommandWithRaftId("test", RaftIdGenerator.NewId())
                        {
                            RaftCommandIndex = 322
                        };

                        var t = leader.ServerStore.Engine.CurrentLeader.PutAsync(cmd, new TimeSpan(2000));
                    });

                    //WaitForUserToContinueTheTest(store);
                    await Task.Delay(3000);
                    long index = 0;
                    long term = 0 ;

                    leader.ServerStore.Engine.GetLastCommitIndex(context, out index, out term);
                    //
                        leader.ServerStore.Engine.StateMachine.skipIndex = index + 1;

                    // foreach (var server in Servers)
                    // {
                    //     server.ServerStore.Engine.GetLastCommitIndex(context, out index, out term);
                    //
                    //     server.ServerStore.Engine.StateMachine.skipIndex = index + 1;
                    // }
                }
                WaitForUserToContinueTheTest(store);
            }
        }
        internal class TestCommandWithRaftId : CommandBase
        {
            private string Name;

#pragma warning disable 649
            private object Value;
#pragma warning restore 649

            public TestCommandWithRaftId(string name, string uniqueRequestId) : base(uniqueRequestId)
            {
                Name = name;
            }

            public override DynamicJsonValue ToJson(JsonOperationContext context)
            {
                var djv = base.ToJson(context);
                djv[nameof(Name)] = Name;
                djv[nameof(Value)] = Value;

                return djv;
            }
        }
        private async Task WaitForAssertion(Action action)
        {
            var sp = Stopwatch.StartNew();
            while (true)
            {
                try
                {
                    action();
                    return;
                }
                catch
                {
                    if (sp.ElapsedMilliseconds > 10_000)
                        throw;

                    await Task.Delay(100);
                }
            }
        }
    }
}
