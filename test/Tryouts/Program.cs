using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using SlowTests.Client.Counters;
using SlowTests.Cluster;
using SlowTests.Core.Utils.Entities;
using SlowTests.Issues;
using SlowTests.Voron;
using Tests.Infrastructure;
using Xunit.Sdk;

namespace Tryouts
{
    public static class Program
    {
        
        public static async Task Main(string[] args)
        {
            using (var store = new DocumentStore() {Database = "db11", Urls = new string[] {"http://127.0.0.1:8082"}})
            {
                 store.Initialize();
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    await session.StoreAsync(new User {Name = "Toli",Id = "aaaaaaa"}, "user/1");
                    await session.SaveChangesAsync();
                }
                // using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                // {
                //     session.Delete("user/1");
                //     await session.SaveChangesAsync();
                //
                // }
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Toli", Count = 5 }, "user/1");
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Toli", Count = 6 }, "user/1");
                    await session.SaveChangesAsync();
                }
                // using (var session = store.OpenAsyncSession())
                // {
                //     await session.StoreAsync(new User { Name = "Toli", Count = 9 }, "user/1");
                //     await session.SaveChangesAsync();
                // }
                // using (var session = store.OpenAsyncSession())
                // {
                //     await session.StoreAsync(new User { Name = "Toli", Count = 53 }, "user/1");
                //     await session.SaveChangesAsync();
                // }
            }
        }
    }
}
