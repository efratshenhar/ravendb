using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Util;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17745 : RavenTestBase
    {
        public RavenDB_17745(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task BulkInsertWithDelay()
        {
            using (var store = GetDocumentStore())
            {
                StreamWithTimeout.DefaultWriteTimeout = TimeSpan.FromSeconds(20);
                StreamWithTimeout.DefaultReadTimeout = TimeSpan.FromSeconds(20);
                var bulk = store.BulkInsert();

                await Task.Delay(StreamWithTimeout.DefaultWriteTimeout + TimeSpan.FromSeconds(5));
                bulk.Store(new User { Name = "Daniel" }, "users/1");
                bulk.Store(new User { Name = "Yael" }, "users/2");

                await Task.Delay(StreamWithTimeout.DefaultWriteTimeout + TimeSpan.FromSeconds(5));
                bulk.Store(new User { Name = "Ido" }, "users/3");
                await Task.Delay(StreamWithTimeout.DefaultWriteTimeout + TimeSpan.FromSeconds(5));
                bulk.Dispose();

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.NotNull(user);
                    Assert.Equal("Daniel", user.Name);

                    user = session.Load<User>("users/2");
                    Assert.NotNull(user);
                    Assert.Equal("Yael", user.Name);

                    user = session.Load<User>("users/3");
                    Assert.NotNull(user);
                    Assert.Equal("Ido", user.Name);
                }
            }
        }

        [Fact]
        public async Task StartStoreInTheMiddleOfAnHeartbeat()
        {
            using (var store = GetDocumentStore())
            {
                StreamWithTimeout.DefaultWriteTimeout = TimeSpan.FromSeconds(20);
                var bulk = store.BulkInsert();

                bulk.ForTestingPurposesOnly().StartStore = () =>
                {
                    bulk.StoreAsync(new User { Name = "Daniel" }, "users/1");
                };

                await Task.Delay(StreamWithTimeout.DefaultWriteTimeout + TimeSpan.FromSeconds(5));

                bulk.Dispose();

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.NotNull(user);
                    Assert.Equal("Daniel", user.Name);
                }
            }
        }


            private WeakReference weakReference;
            [Fact]
            public async Task killTimerWithGC()
            {
                using (var store = GetDocumentStore())
                {
                    StreamWithTimeout.DefaultWriteTimeout = TimeSpan.FromSeconds(1000);
                    DoBulkInsert(store);
                }
                //await Task.Delay(StreamWithTimeout.DefaultWriteTimeout + TimeSpan.FromSeconds(5));
                await Task.Delay( TimeSpan.FromSeconds(35));
                Console.WriteLine("Start GC");
                for (var i = 0; i < 20; i++)
                {
                    await Task.Delay(TimeSpan.FromSeconds(1));
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
        
                    // if (weakReference.Target == null)
                    //     break;
                }
                Console.WriteLine("End GC");

                Console.WriteLine("Start 2nd run");
            using (var store = GetDocumentStore())
                {
                    StreamWithTimeout.DefaultWriteTimeout = TimeSpan.FromSeconds(1000);
                    DoBulkInsert(store);
                }
                //await Task.Delay(StreamWithTimeout.DefaultWriteTimeout + TimeSpan.FromSeconds(5));
                await Task.Delay(TimeSpan.FromSeconds(35));
                Console.WriteLine("Start GC");
                for (var i = 0; i < 20; i++)
                {
                    await Task.Delay(TimeSpan.FromSeconds(1));
                    GC.Collect();
                    GC.WaitForPendingFinalizers();

                    // if (weakReference.Target == null)
                    //     break;
                }
                Console.WriteLine("End GC");

            // Console.WriteLine("before delay");
            await Task.Delay(TimeSpan.FromSeconds(35));
                 Console.WriteLine("End test");
                //Assert.Null(weakReference.Target);
        }
        
            private void DoBulkInsert(DocumentStore store)
            {
                var bulk = store.BulkInsert();
                
            }
        }
    
}
