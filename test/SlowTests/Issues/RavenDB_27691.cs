using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
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

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task IdleDatabaseLoadedBeforeItsBackupStatusSaveEndsShouldNotGetWakeupTimer()
        {
            using var server = GetNewServer();
            using var store = GetDocumentStore(new Options { Server = server });
            var (taskId, lastEtag) = await CreateBackedUpDatabaseAsync(server, store);
            var landlord = server.ServerStore.DatabasesLandlord;
            var (saved, release) = HoldIdleBackupStatusSave(landlord);

            try
            {
                MakeIdle(server, store.Database, new IdleDatabaseActivity(IdleDatabaseActivityType.UpdateBackupStatusOnly, DateTime.UtcNow, taskId, lastEtag));
                Assert.True(await WaitAsync(saved.Task), "The idle backup status save didn't start");

                await landlord.TryGetOrCreateResourceStore(store.Database);
                Assert.False(WaitForValue(() => server.ServerStore.IdleDatabases.ContainsKey(store.Database), false));

                release.SetResult(null);

                Assert.False(landlord.TryGetWakeupActivity(store.Database, out var activity), $"A wakeup timer of type {activity?.Type} at {activity?.DateTime} was set for a loaded database");
            }
            finally
            {
                release.TrySetResult(null);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task IdleDatabaseLoadingWhileItsBackupStatusSaveEndsShouldNotGetWakeupTimer()
        {
            using var server = GetNewServer();
            using var store = GetDocumentStore(new Options { Server = server });
            var (taskId, lastEtag) = await CreateBackedUpDatabaseAsync(server, store);
            var landlord = server.ServerStore.DatabasesLandlord;
            var (saved, release) = HoldIdleBackupStatusSave(landlord);
            using var loadGate = new ManualResetEventSlim(false);

            try
            {
                MakeIdle(server, store.Database, new IdleDatabaseActivity(IdleDatabaseActivityType.UpdateBackupStatusOnly, DateTime.UtcNow, taskId, lastEtag));
                Assert.True(await WaitAsync(saved.Task), "The idle backup status save didn't start");

                landlord.ForTestingPurposesOnly().OnBeforeDocumentDatabaseInitialization = _ => loadGate.Wait(TimeSpan.FromMinutes(1));
                var loadTask = landlord.TryGetOrCreateResourceStore(store.Database);
                Assert.True(WaitForValue(() => landlord.DatabasesCache.TryGetValue(store.Database, out _), true));

                release.SetResult(null);
                loadGate.Set();
                await loadTask;
                Assert.False(WaitForValue(() => server.ServerStore.IdleDatabases.ContainsKey(store.Database), false));

                Assert.False(landlord.TryGetWakeupActivity(store.Database, out var activity), $"A wakeup timer of type {activity?.Type} at {activity?.DateTime} was set for a loaded database");
            }
            finally
            {
                release.TrySetResult(null);
                loadGate.Set();
                landlord.ForTestingPurposesOnly().OnBeforeDocumentDatabaseInitialization = null;
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task IdleDatabaseUnloadedAgainBeforeItsBackupStatusSaveEndsShouldKeepItsNewWakeupTimer()
        {
            using var server = GetNewServer();
            using var store = GetDocumentStore(new Options { Server = server });
            var (taskId, lastEtag) = await CreateBackedUpDatabaseAsync(server, store);
            var landlord = server.ServerStore.DatabasesLandlord;
            var (saved, release) = HoldIdleBackupStatusSave(landlord);

            try
            {
                MakeIdle(server, store.Database, new IdleDatabaseActivity(IdleDatabaseActivityType.UpdateBackupStatusOnly, DateTime.UtcNow, taskId, lastEtag));
                Assert.True(await WaitAsync(saved.Task), "The idle backup status save didn't start");

                await landlord.TryGetOrCreateResourceStore(store.Database);
                Assert.False(WaitForValue(() => server.ServerStore.IdleDatabases.ContainsKey(store.Database), false));

                var newActivity = new IdleDatabaseActivity(IdleDatabaseActivityType.WakeUpDatabase, DateTime.UtcNow.AddMinutes(10));
                MakeIdle(server, store.Database, newActivity);

                release.SetResult(null);

                Assert.True(landlord.TryGetWakeupActivity(store.Database, out var activity), "The new wakeup timer was removed");
                Assert.Equal(IdleDatabaseActivityType.WakeUpDatabase, activity.Type);
                Assert.Equal(newActivity.DateTime, activity.DateTime);
            }
            finally
            {
                release.TrySetResult(null);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task IdleDatabaseThatFailedToLoadShouldGetWakeupTimerAgain()
        {
            using var server = GetNewServer();
            using var store = GetDocumentStore(new Options { Server = server });
            await CreateBackedUpDatabaseAsync(server, store);
            var landlord = server.ServerStore.DatabasesLandlord;

            try
            {
                MakeIdle(server, store.Database, new IdleDatabaseActivity(IdleDatabaseActivityType.WakeUpDatabase, DateTime.UtcNow.AddHours(1)));

                landlord.ForTestingPurposesOnly().OnBeforeDocumentDatabaseInitialization = _ => throw new InvalidOperationException("Simulated load failure");
                await Assert.ThrowsAnyAsync<Exception>(() => landlord.TryGetOrCreateResourceStore(store.Database));

                Assert.True(WaitForValue(() => landlord.TryGetWakeupActivity(store.Database, out _), true), "The idle database was left without a wakeup timer after its load failed");
            }
            finally
            {
                landlord.ForTestingPurposesOnly().OnBeforeDocumentDatabaseInitialization = null;
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task WakeupOfIdleDatabaseThatCannotLoadShouldReleaseItsLoadingSlot()
        {
            using var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Backup.MaxNumberOfConcurrentBackups)] = "1"
                }
            });
            using var store = GetDocumentStore(new Options { Server = server });
            var landlord = server.ServerStore.DatabasesLandlord;
            var failure = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);

            MakeIdle(server, store.Database, new IdleDatabaseActivity(IdleDatabaseActivityType.WakeUpDatabase, DateTime.UtcNow.AddHours(1)));

            await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, disable: true));
            Assert.True(await WaitForValueAsync(async () => (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).Disabled, true));
            Assert.True(landlord.TryGetWakeupActivity(store.Database, out _), "The wakeup timer was removed when the database was disabled");

            landlord.ForTestingPurposesOnly().OnFailedRescheduleNextScheduledActivity = (e, databaseName) =>
            {
                if (databaseName == store.Database)
                    failure.TrySetResult(e);
            };

            landlord.RescheduleNextIdleDatabaseActivity(store.Database, new IdleDatabaseActivity(IdleDatabaseActivityType.WakeUpDatabase, DateTime.UtcNow));
            Assert.True(await WaitAsync(failure.Task), "The wakeup of the disabled database didn't fail");

            using (var slot = server.ServerStore.ConcurrentBackupsCounter.TryStartDatabaseForBackup())
                Assert.True(slot != null, $"The slot for loading a database for a backup was not released after: {failure.Task.Result}");

            await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, disable: false));
        }

        private async Task<(long TaskId, long LastEtag)> CreateBackedUpDatabaseAsync(RavenServer server, DocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Grisha" }, "users/1");
                await session.SaveChangesAsync();
            }

            var config = Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "0 0 1 1 *", incrementalBackupFrequency: "30 * * * *");
            var taskId = await Backup.UpdateConfigAndRunBackupAsync(server, config, store);

            var database = await Databases.GetDocumentDatabaseInstanceFor(server, store);
            var activity = database.PeriodicBackupRunner.GetNextIdleDatabaseActivity(store.Database);
            Assert.Equal(IdleDatabaseActivityType.UpdateBackupStatusOnly, activity.Type);

            return (taskId, activity.LastEtag);
        }

        private static void MakeIdle(RavenServer server, string databaseName, IdleDatabaseActivity activity)
        {
            var landlord = server.ServerStore.DatabasesLandlord;
            landlord.ForTestingPurposesOnly().SkipShouldContinueDisposeCheck = true;
            server.ServerStore.IdleDatabases[databaseName] = new Dictionary<string, long>();
            Assert.True(landlord.UnloadDirectly(databaseName, activity), $"Failed to unload database '{databaseName}'");
        }

        private static (TaskCompletionSource<object> Saved, TaskCompletionSource<object> Release) HoldIdleBackupStatusSave(DatabasesLandlord landlord)
        {
            var saved = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource<object>();

            landlord.ForTestingPurposesOnly().AfterIdleBackupStatusSaved = async _ =>
            {
                saved.TrySetResult(null);
                await release.Task;
            };

            return (saved, release);
        }

        private static async Task<bool> WaitAsync(Task task)
        {
            return await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(30))) == task;
        }
    }
}
