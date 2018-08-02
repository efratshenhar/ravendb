using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Client.Indexing
{
    public class TestIndexesFromClient : RavenTestBase
    {
        private class UsersByName : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "Users_ByName",
                    Maps = { "from user in docs.Users select new { user.Name }" },
                    Type = IndexType.Map,
                    IsTestIndex = true,
                    NumberOfEntriesToTest = 1
                };
            }
        }
        [Fact]
        public void Can_Put()
        {
            using (var store = GetDocumentStore())
            {
                
                using (var session = store.OpenSession())
                {
                     session.Store(new User { Name = "Fitzchak" });
                     session.Store(new User { Name = "Arek" });

                    session.SaveChanges();
                    store.ExecuteIndex(new UsersByName());
                    WaitForIndexing(store);
                    var res = session.Query<User>("UsersByName").ToList();
                    Assert.Equal(res.Count,1);
                }

                
            }
        }

        public class UserAndAge
        {
            public string Name { set; get; }
            public int Age { set; get; }
        }

    }
}
