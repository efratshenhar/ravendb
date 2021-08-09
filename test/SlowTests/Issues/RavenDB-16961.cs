using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Graph;
using FastTests.Utils;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16961 : RavenTestBase
    {
        public RavenDB_16961(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task StripRevisionFlagFromTombstone()
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, new RevisionsConfiguration()
                {
                    Default = new RevisionsCollectionConfiguration()
                    {
                        Disabled = false
                    }
                });
                var user = new User() {Name = "Toli"};
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 3; i++)
                    {
                        user.Age = i;
                        await session.StoreAsync(user, "users/1");
                        await session.SaveChangesAsync();
                    }
                    session.Delete("users/1");
                    await session.SaveChangesAsync();
                }

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, new RevisionsConfiguration());

                var db = await GetDocumentDatabaseInstanceFor(store, store.Database);
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                    await db.DocumentsStorage.RevisionsStorage.EnforceConfiguration(_ => { }, token);


                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var tombstone = db.DocumentsStorage.GetDocumentOrTombstone(ctx, "users/1");
                    Assert.False(tombstone.Tombstone.Flags.Contain(DocumentFlags.HasRevisions));
                }

            }
        }

    }
}
