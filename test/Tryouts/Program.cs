using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session;
using SlowTests.Core.Utils.Entities;

namespace Tryouts
{
    public static class Program
    {
       
        public static async Task Main(string[] args)
        {
            using (var store = new DocumentStore() { Database = "Test1", Urls = new[] { "http://127.0.0.1:8080" } })
            {
                store.Initialize();
                 //InsertCameraData(store);
                 //CreateCameraCostIndex(store);
                 using (var session = store.OpenSession())
                 {
                    var facets = GetFacets();
                    
                    var fList = new List<string> { "Tripod3", "Low Light Compatible" , "Fixed Lens21" };
                    using (var s = store.OpenSession())
                    {
                        var facetResults = s.Query<Camera, CameraCostIndex>()
                            .Where(x => x.AdvancedFeatures != null && x.AdvancedFeatures.Contains<string>(fList.GetEnumerator().ToString(), StringComparer.Ordinal))
                            .AggregateBy(facets)
                            .Execute();
                    }
                    // using (var s = store.OpenSession())
                    // {
                    //     var expressions = new Expression<Func<Camera, bool>>[]
                    //     {
                    //         x => x.Cost >= 100 && x.Cost <= 300, x => x.DateOfListing > new DateTime(2000, 1, 1), 
                    //         x => x.Megapixels > 5.0m && x.Cost < 500,
                    //         x => x.Manufacturer == "abc&edf",
                    //         x => x.AdvancedFeatures != null && x.AdvancedFeatures.Contains<string>(fList.GetEnumerator().ToString(), StringComparer.Ordinal)
                    //     };
                    //
                    //     foreach (var exp in expressions)
                    //     {
                    //         var facetResults = s.Query<Camera, CameraCostIndex>()
                    //             .Where(exp)
                    //             .AggregateBy(facets)
                    //             .Execute();
                    //     }
                    // }
                    // var results = session.Query<Camera>()
                    // .GroupBy(x => new
                    // {
                    //     x.Manufacturer,
                    //     x.Cost
                    // }).Select(x => new
                    // {
                    //     x.Key.Manufacturer,
                    //     Count = x.Key.Cost
                    // })
                    // .ToList();


                }
            }
        }
       public static void CreateCameraCostIndex(IDocumentStore store)
        {
            var index = new CameraCostIndex();

            store.Maintenance.Send(new PutIndexesOperation(new[] { index.CreateIndexDefinition() }));
        }

        public class CameraCostIndex : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                            {
                                @"from camera in docs 
                                select new 
                                { 
                                    camera.Manufacturer, 
                                    camera.Model, 
                                    camera.Cost,
                                    camera.DateOfListing,
                                    camera.Megapixels,
                                    camera.AdvancedFeatures
                                }"
                            },
                    Name = "CameraCost"
                };
            }

            public override string IndexName => new CameraCostIndex().CreateIndexDefinition().Name;
        }

        public static void InsertCameraData(IDocumentStore store)
        {
            using (var session = store.BulkInsert())
            {
                for (int i = 2000000; i <= 5000000; i++)
                {
                    var AdvancedFeaturesList = new List<string>();
                    var end = Random.Next(1, 30);
                    for (int j = 0; j < end; j++)
                    {
                        AdvancedFeaturesList.Add(Features[Random.Next(1, 16)]);
                    }
                    var cam = new Camera
                    {
                        Id = i.ToString(),
                        DateOfListing = new DateTime(1980 + Random.Next(1, 30), Random.Next(1, 12), Random.Next(1, 27)),
                        Manufacturer = Manufacturers[(int)(Random.NextDouble() * Manufacturers.Count)],
                        Model = Models[(int)(Random.NextDouble() * Models.Count)],
                        Cost = (int)(decimal)((Random.NextDouble() * 900.0) + 100.0),    //100.0 to 1000.0
                        Zoom = (int)(Random.NextDouble() * 12) + 2,                 //2.0 to 12.0
                        Megapixels = (decimal)((Random.NextDouble() * 10.0) + 1.0), //1.0 to 11.0
                        ImageStabilizer = Random.NextDouble() > 0.6,
                        AdvancedFeatures = AdvancedFeaturesList
                    };
                    session.Store(cam);

                }
                
            }
        }

        public static List<FacetBase> GetFacets()
        {
            return new List<FacetBase>
            {
                new Facet<Camera> {FieldName = x => x.Manufacturer},
                new RangeFacet<Camera>
                {
                    Ranges =
                    {
                        x => x.Cost <= 200m,
                        x => x.Cost >= 200m && x.Cost <= 400m,
                        x => x.Cost >= 400m && x.Cost <= 600m,
                        x => x.Cost >= 600m && x.Cost <= 800m,
                        x => x.Cost >= 800m
                    }
                },
                new RangeFacet<Camera>
                {
                    Ranges =
                    {
                        x => x.Megapixels <= 3.0m,
                        x => x.Megapixels >= 3.0m && x.Megapixels <= 7.0m,
                        x => x.Megapixels >= 7.0m && x.Megapixels <= 10.0m,
                        x => x.Megapixels >= 10.0m
                    }
                }
            };
        }

        private static readonly List<string> Features = new List<string>
                {
                    "Image Stabilizer",
                    "Tripod",
                    "Low Light Compatible",
                    "Fixed Lens",
                    "LCD",
                    "Image Stabilizer2",
                    "Tripod2",
                    "Low Light Compatible2",
                    "Fixed Lens2",
                    "LCD2",
                    "Image Stabilizer3",
                    "Tripod3",
                    "Low Light Compatible3",
                    "Fixed Lens3",
                    "LC3D",
                    "Image Stabilizer21",
                    "Tripod21",
                    "Low Light Compatible21",
                    "Fixed Lens21",
                    "LCD21"
                };

        private static readonly List<string> Manufacturers = new List<string>
                {
                    "Sony",
                    "Nikon",
                    "Phillips",
                    "Canon",
                    "Jessops"
                };

        private static readonly List<string> Models = new List<string>
                {
                    "Model1",
                    "Model2",
                    "Model3",
                    "Model4",
                    "Model5"
                };

        private static readonly Random Random = new Random(1337);



        public class Camera
        {
            public string Id { get; set; }

            public DateTime DateOfListing { get; set; }
            public string Manufacturer { get; set; }
            public string Model { get; set; }
            public decimal Cost { get; set; }

            public int Zoom { get; set; }
            public decimal Megapixels { get; set; }
            public bool ImageStabilizer { get; set; }
            public List<String> AdvancedFeatures { get; set; }



        }

        public static IList<Camera> GetCameras(int numCameras)
        {
            var cameraList = new List<Camera>(numCameras);
           

            
            for (int i = 1; i <= numCameras; i++)
            {
                var AdvancedFeaturesList = new List<string>();
                var end = Random.Next(1, 30);
                for (int j = 0; j < end; j++)
                {
                    AdvancedFeaturesList.Add(Features[Random.Next(1,16)]);
                }
                cameraList.Add(new Camera
                {
                    Id = i.ToString(),
                    DateOfListing = new DateTime(1980 + Random.Next(1, 30), Random.Next(1, 12), Random.Next(1, 27)),
                    Manufacturer = Manufacturers[(int)(Random.NextDouble() * Manufacturers.Count)],
                    Model = Models[(int)(Random.NextDouble() * Models.Count)],
                    Cost = (int)(decimal)((Random.NextDouble() * 900.0) + 100.0),    //100.0 to 1000.0
                    Zoom = (int)(Random.NextDouble() * 12) + 2,                 //2.0 to 12.0
                    Megapixels = (decimal)((Random.NextDouble() * 10.0) + 1.0), //1.0 to 11.0
                    ImageStabilizer = Random.NextDouble() > 0.6,
                    AdvancedFeatures = AdvancedFeaturesList
                });
            }

            return cameraList;
        }
    }
}
