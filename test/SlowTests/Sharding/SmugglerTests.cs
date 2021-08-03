using System;
using System.IO;
using System.Threading.Tasks;
using FastTests.Sharding;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding
{
    public class SmugglerTests : ShardedTestBase
    {
        public SmugglerTests(ITestOutputHelper output) : base(output)
        {
        }
        private static async Task InsertDataForShard(IDocumentStore store1, string[] names)
        {
            using (var session = store1.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1", Age = 5 }, "users/1");
                await session.StoreAsync(new User { Name = "Name2", LastName = "LastName2", Age = 78 }, "users/2");
                await session.StoreAsync(new User { Name = "Name1", LastName = "LastName3", Age = 4 }, "users/3");
                await session.StoreAsync(new User { Name = "Name2", LastName = "LastName4", Age = 15 }, "users/4");

                session.TimeSeriesFor("users/1", "Heartrate")
                    .Append(DateTime.Now, 59d, "watches/fitbit");
                session.TimeSeriesFor("users/3", "Heartrate")
                    .Append(DateTime.Now.AddHours(6), 59d, "watches/fitbit");
                
                session.CountersFor("users/2").Increment("Downloads", 100);
                
                await using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                await using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                await using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                {
                    session.Advanced.Attachments.Store("users/1", names[0], backgroundStream, "ImGgE/jPeG");
                    session.Advanced.Attachments.Store("users/2", names[1], fileStream);
                    session.Advanced.Attachments.Store("users/3", names[2], profileStream, "image/png");
                     await session.SaveChangesAsync();
                }
            }

            using (var session = store1.OpenAsyncSession())
            {
                session.Delete("users/4");

                await session.SaveChangesAsync();
            }
        }

        private static async Task InsertData(IDocumentStore store1, string[] names, RavenServer server)
        {
            using (var session = store1.OpenAsyncSession())
            {
                await RevisionsHelper.SetupRevisions(store1, server.ServerStore, new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false
                    }
                });

                await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1", Age = 5 }, "users/1");
                await session.StoreAsync(new User { Name = "Name2", LastName = "LastName2", Age = 78 }, "users/2");
                await session.StoreAsync(new User { Name = "Name1", LastName = "LastName3", Age = 4 }, "users/3");
                await session.StoreAsync(new User { Name = "Name2", LastName = "LastName4", Age = 15 }, "users/4");

                session.TimeSeriesFor("users/1", "Heartrate")
                    .Append(DateTime.Now, 59d, "watches/fitbit");
                session.TimeSeriesFor("users/3", "Heartrate")
                    .Append(DateTime.Now.AddHours(6), 59d, "watches/fitbit");

                session.CountersFor("users/2").Increment("Downloads", 100);

                await using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                await using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                await using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                {
                    session.Advanced.Attachments.Store("users/1", names[0], backgroundStream, "ImGgE/jPeG");
                    session.Advanced.Attachments.Store("users/2", names[1], fileStream);
                    session.Advanced.Attachments.Store("users/3", names[2], profileStream, "image/png");
                    await session.SaveChangesAsync();
                }
            }

            using (var session = store1.OpenAsyncSession())
            {
                session.Delete("users/4");
                var user = await session.LoadAsync<User>("users/1");
                user.Age = 10;
                await session.SaveChangesAsync();
            }
        }
        private static async Task CheckData(IDocumentStore store2, string[] names)
        {
            var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
            Assert.Equal(3, stats.CountOfDocuments);

            using (var session = store2.OpenSession())
            {
                var val = session.TimeSeriesFor("users/1", "Heartrate")
                    .Get(DateTime.MinValue, DateTime.MaxValue);

                Assert.Equal(1, val.Length);

                val = session.TimeSeriesFor("users/3", "Heartrate")
                    .Get(DateTime.MinValue, DateTime.MaxValue);

                Assert.Equal(1, val.Length);

                var counterValue = session.CountersFor("users/2").Get("Downloads");
                Assert.Equal(100, counterValue.Value);
            }

            using (var session = store2.OpenAsyncSession())
            {
                for (var i = 0; i < names.Length; i++)
                {
                    var user = await session.LoadAsync<User>("users/" + (i + 1));
                    var metadata = session.Advanced.GetMetadataFor(user);
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    Assert.Equal(1, attachments.Length);
                    var attachment = attachments[0];
                    Assert.Equal(names[i], attachment.GetString(nameof(AttachmentName.Name)));
                    var hash = attachment.GetString(nameof(AttachmentName.Hash));
                    if (i == 0)
                    {
                        Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", hash);
                        Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                    }
                    else if (i == 1)
                    {
                        Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", hash);
                        Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                    }
                    else if (i == 2)
                    {
                        Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", hash);
                        Assert.Equal(3, attachment.GetLong(nameof(AttachmentName.Size)));
                    }
                }
            }
        }

        private async Task CheckDataForShard(IDocumentStore store2, string[] names)
        {
            //var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
            //Assert.Equal(3, stats.CountOfDocuments);

            using (var session = store2.OpenSession())
            {
                for (int i = 1; i < 4; i++)
                {
                    var user = session.Load<User>("users/1");
                    Assert.NotNull(user);
                }

                // var rangeResult = store2.Operations.Send(
                //     new GetTimeSeriesOperation("users/1", "Heartrate", DateTime.MinValue, DateTime.MaxValue));
                //
                //
                // var val = session.TimeSeriesFor("users/1", "Heartrate")
                //     .Get(DateTime.MinValue, DateTime.MaxValue);

                // Assert.Equal(1, val.Length);
                //
                // val = session.TimeSeriesFor("users/3", "Heartrate")
                //     .Get(DateTime.MinValue, DateTime.MaxValue);
                //
                // Assert.Equal(1, val.Length);

                // var counterValue = session.CountersFor("users/2").Get("Downloads");
                // Assert.Equal(100, counterValue.Value);
            }

            using (var session = store2.OpenAsyncSession())
            {
                for (var i = 1; i <= names.Length; i++)
                {
                    var user = await session.LoadAsync<User>("users/" + (i));
                    var metadata = session.Advanced.GetMetadataFor(user);
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    Assert.Equal(1, attachments.Length);
                    var attachment = attachments[0];
                    Assert.Equal(names[i- 1], attachment.GetString(nameof(AttachmentName.Name)));
                    var hash = attachment.GetString(nameof(AttachmentName.Hash));
                    var ts = metadata.TryGetValue(Constants.Documents.Metadata.TimeSeries, out object _);
                    var counter = metadata.TryGetValue(Constants.Documents.Metadata.Counters, out object  _);
                    var flag = metadata.TryGetValue(Constants.Documents.Metadata.Flags, out string flags);

                    if (i == 1)
                    {
                        Assert.True(ts);
                        Assert.False(counter);
                        Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", hash);
                        Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                        Assert.Contains(nameof(DocumentFlags.HasRevisions), flags);
                    }
                    else if (i == 2)
                    {
                        Assert.True(counter);
                        Assert.False(false);
                        Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", hash);
                        Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                    }
                    else if (i == 3)
                    {
                        Assert.True(ts);
                        Assert.False(counter);
                        Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", hash);
                        Assert.Equal(3, attachment.GetLong(nameof(AttachmentName.Size)));
                    }
                }

                var documentDatabase = Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store2.Database);
                foreach (var dd in documentDatabase)
                {
                    using (dd.Result.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var tomb = dd.Result.DocumentsStorage.GetDocumentOrTombstone(context, "users/4").Tombstone;
                        if (tomb != null)
                            break;
                    }
                    Assert.True(false);
                }
               
            }
        }
  
        [Fact]
        public async Task ExportFromShardToRegular()
        {
            var file = GetTempFileName();
            var names = new[]
            {
                "background-photo.jpg",
                "fileNAME_#$1^%_בעברית.txt",
                "profile.png",
            };
            try
            {
                using (var store1 = GetShardedDocumentStore())
                {
                    await InsertDataForShard(store1, names);

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    //await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1)); // TODO - Doesn't work with shard DB
                    await Task.Delay(TimeSpan.FromSeconds(20));

                    using (var store2 = GetDocumentStore(new Options { ModifyDatabaseName = s => $"{s}_2" }))
                    {
                        operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                        
                        await CheckData(store2, names);
                    }

                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task ExportFromRegularToShard()
        {
            var file = GetTempFileName();
            var names = new[]
            {
                "background-photo.jpg",
                "fileNAME_#$1^%_בעברית.txt",
                "profile.png",
            };
            try
            {
                using (var store1 = GetDocumentStore(new Options { ModifyDatabaseName = s => $"{s}_2" }))
                {
                    
                    await InsertData(store1, names, Server);

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1)); //TODO - EFRAT
                    
                    using (var store2 = GetShardedDocumentStore())
                    {
                        operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                        {
                            OperateOnTypes = DatabaseItemType.Documents | 
                                             DatabaseItemType.TimeSeries | 
                                             DatabaseItemType.CounterGroups | 
                                             DatabaseItemType.Attachments | 
                                             //DatabaseItemType.RevisionDocuments |
                                             DatabaseItemType.Tombstones
                        }, file);
                        //await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1)); // TODO - Doesn't work with shard DB
                        await Task.Delay(TimeSpan.FromSeconds(20));
                        WaitForUserToContinueTheTest(store2);
                        await CheckDataForShard(store2, names); 
                    }

                }
            }
            finally
            {
                File.Delete(file);
            }
        }

       
        [Fact]
        public async Task ExportFromShardToRegularEncrypted()
        {
            var file = GetTempFileName();
            var names = new[]
            {
                "background-photo.jpg",
                "fileNAME_#$1^%_בעברית.txt",
                "profile.png",
            };
            try
            {
                using (var store1 = GetShardedDocumentStore())
                {
                    await InsertDataForShard(store1, names);

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions
                    {
                        EncryptionKey = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs="
                    }, file);
                    //await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                    await Task.Delay(TimeSpan.FromSeconds(20));
                    using (var store2 = GetDocumentStore(new Options { ModifyDatabaseName = s => $"{s}_2" }))
                    {
                        operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions { EncryptionKey = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs=" },
                            file);
                        //WaitForUserToContinueTheTest(store2);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        await CheckData(store2, names);
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task ExportFromRegularToShardEncrypted()
        {
            var file = GetTempFileName();
            var names = new[]
            {
                "background-photo.jpg",
                "fileNAME_#$1^%_בעברית.txt",
                "profile.png",
            };
            try
            {
                using (var store1 = GetDocumentStore(new Options { ModifyDatabaseName = s => $"{s}_2" }))
                {

                    await InsertData(store1, names, Server);

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions
                    {
                        EncryptionKey = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs="
                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1)); //TODO - EFRAT

                    using (var store2 = GetShardedDocumentStore())
                    {
                        operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions
                        {
                            EncryptionKey = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs=",
                            OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.Attachments | DatabaseItemType.TimeSeries | DatabaseItemType.CounterGroups
                        }, file);
                        WaitForUserToContinueTheTest(store2);
                        //await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1)); // TODO - Doesn't work with shard DB
                        await Task.Delay(TimeSpan.FromSeconds(20));
                        await CheckDataForShard(store2, names);
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

    }
}
