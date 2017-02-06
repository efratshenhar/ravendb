using System.Linq;
using FastTests;
using Raven.NewClient.Abstractions;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Operations.Databases.Indexes;
using SlowTests.Utils;
using Xunit;

namespace SlowTests.Tests.Bugs.Zhang
{
    public class UseMaxForLongTypeInReduce : RavenNewTestBase
    {
        private const string Map = @"
from doc in docs.Items
from tag in doc.Tags
select new { Name = tag.Name, CreatedTimeTicks = doc.CreatedTimeTicks }
";

        private const string Reduce = @"
from agg in results
group agg by agg.Name into g
let createdTimeTicks = g.Max(x => (long)x.CreatedTimeTicks)
select new {Name = g.Key, CreatedTimeTicks = createdTimeTicks}
";

        private class Item
        {
            public string Id { get; set; }

            public string Topic { get; set; }

            public Tag[] Tags { get; set; }

            public long CreatedTimeTicks { get; set; }
        }

        private class Tag
        {
            public string Name { get; set; }
        }

        [Fact]
        public void CanUseMax()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Topic = "RavenDB is Hot", CreatedTimeTicks = SystemTime.UtcNow.Ticks, Tags = new[] { new Tag { Name = "DB" }, new Tag { Name = "NoSQL" } } });

                    session.Store(new Item { Topic = "RavenDB is Fast", CreatedTimeTicks = SystemTime.UtcNow.AddMinutes(10).Ticks, Tags = new[] { new Tag { Name = "NoSQL" } } });

                    session.SaveChanges();
                }

                store.Admin.Send(new PutIndexOperation("test",
                    new IndexDefinition
                    {
                        Maps = { Map },
                        Reduce = Reduce,
                    }));

                using (var session = store.OpenSession())
                {
                    session.Advanced.DocumentQuery<object>("test").WaitForNonStaleResults().ToArray<object>();
                }

                TestHelper.AssertNoIndexErrors(store);
            }
        }
    }
}
