using System;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Tests.Core.Utils.Entities;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var store = new DocumentStore()
            {
                Url = "http://localhost.fiddler:8080",
                DefaultDatabase = "Temp"
            };
            store.Initialize();
           /* store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
            {
                Id = "Temp",
                Settings = { { "Raven/DataDir", @"~\Databases\Temp"} }
            });*/
            using (var session = store.OpenSession())
            {
                session.Store(new User()
                {
                    Name = "A"
                } , "users/4");
                session.SaveChanges();
            };
            using (var session = store.OpenSession())
            {
                session.Store(new User()
                {
                    Name = "B"
                }, "users/3");
                var user = session.Load<User>("users/3");
                user.Name = "Mitzi";
                session.SaveChanges();
            };
            using (var session = store.OpenNewSession())
            {
                var user = session.Load<User>("users/4");
                var user2 = session.Load<User>("users/3");
                //user.Name = "Mitzi";
                session.SaveChanges();
            };
            store.Dispose();

            /*var newStore = new DocumentStore()
            {
                Url = "http://localhost.fiddler:8081",
                DefaultDatabase = "Temp"
            };
            store.Initialize();*/
            /*store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
            {
                Id = "Temp",
                Settings = { { "Raven/DataDir", @"~\Databases\Temp"} }
            });*/

            // newStore.Dispose();
        }
    }
}

