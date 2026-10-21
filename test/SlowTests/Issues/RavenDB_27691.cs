using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Utils;
using Sparrow.Logging;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_27691 : RavenTestBase
    {
        public RavenDB_27691(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.BackupExportImport)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task SavingBackupStatusOfDeletedDatabaseShouldNotLeaveLocalStatus(bool hardDelete)
        {
            using (var store = GetDocumentStore())
            {
                var config = Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder"));
                var taskId = await Backup.UpdateConfigAsync(Server, config, store);

                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, hardDelete));
                var deleted = await WaitForValueAsync(async () => await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)) == null, true);
                Assert.True(deleted, $"Database '{store.Database}' was not deleted");

                var status = new PeriodicBackupStatus
                {
                    TaskId = taskId,
                    NodeTag = Server.ServerStore.NodeTag,
                    BackupType = BackupType.Backup,
                    LastFullBackup = DateTime.UtcNow,
                    LastFullBackupInternal = DateTime.UtcNow
                };

                await BackupUtils.SaveBackupStatusAsync(status, store.Database, Server.ServerStore, LoggingSource.Instance.GetLogger<RavenDB_27691>(store.Database));

                Assert.Null(Server.ServerStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(store.Database, taskId));
            }
        }
    }
}
