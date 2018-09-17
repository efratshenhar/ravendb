using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_11764 : RavenTest
    {
        [Fact]
        public void RavenPagingBug()
        {
            using (var store = NewRemoteDocumentStore(true))
            {
                CreateEmployees("CH", store);
                CreateEmployees("IT", store);
                CreateEmployees("ES", store);

                //Assert.Equal(LoadEmployeesWithoutPagingInformation("CH*|ES*", store).Count(), 20);
                //Assert.Equal(LoadEmployeesWithoutPagingInformation("CH*|IT*", store).Count(), 20);
                //Assert.Equal(LoadEmployeesWithoutPagingInformation("ES*|IT*", store).Count(), 20);

                Assert.Equal(LoadEmployeesWithPagingInformation("CH*|ES*", store).Count(), 20);
                Assert.Equal(LoadEmployeesWithPagingInformation("CH*|IT*", store).Count(), 20);
                //Assert.Equal(LoadEmployeesWithPagingInformation("ES*|IT*", store).Count(), 20);
            }

        }

        public static IEnumerable<Employee> LoadEmployeesWithPagingInformation(string matches, DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var start = 0;
                int pageSize = 15;
                var allDocuments = new List<Employee>();
                var pagingInformation = new RavenPagingInformation();
                IEnumerable<Employee> documents;

                do
                {
                    documents = session.Advanced.LoadStartingWith<Employee>("Employee/", matches: matches, start: start, pageSize: pageSize, pagingInformation: pagingInformation);
                    allDocuments.AddRange(documents);
                    start += pageSize;
                    session.Advanced.MaxNumberOfRequestsPerSession++;
                }
                while (documents.Any());

                return allDocuments;
            }
        }

        public static IEnumerable<Employee> LoadEmployeesWithoutPagingInformation(string matches, DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var start = 0;
                int pageSize = 15;
                var allDocuments = new List<Employee>();
                IEnumerable<Employee> documents;

                do
                {
                    documents = session.Advanced.LoadStartingWith<Employee>("Employee/", matches: matches, start: start, pageSize: pageSize);
                    allDocuments.AddRange(documents);
                    start += pageSize;
                    session.Advanced.MaxNumberOfRequestsPerSession++;
                }
                while (documents.Any());

                return allDocuments;
            }
        }

        public static void CreateEmployees(string country, DocumentStore store)
        {
            using (IDocumentSession session = store.OpenSession())
            {
                for (int i = 0; i < 10; ++i)
                {
                    session.Store(new Employee() { Id = "Employee/" + country + i, Name = "Bob" });
                }
                session.SaveChanges();
            }
        }


        public class Employee
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }
    }
}
