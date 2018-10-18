using System;
using System.Threading.Tasks;
using FastTests.Server.Documents.Queries.Parser;
using FastTests.Voron.Backups;
using FastTests.Voron.Compaction;
using RachisTests.DatabaseCluster;
using SlowTests.Authentication;
using SlowTests.Bugs.MapRedue;
using SlowTests.Client;
using SlowTests.Client.Attachments;
using SlowTests.Issues;
using SlowTests.MailingList;
using Sparrow.Logging;
using StressTests.Client.Attachments;

namespace Tryouts
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            try
            {
                using (var test = new ReplicationTests())
                {
                    await test.DoNotReplicateBack(true);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

            
        }
    }
}
