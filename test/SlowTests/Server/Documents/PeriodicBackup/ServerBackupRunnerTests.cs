using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Backups;
using Raven.Server.ServerWide.Backups.Policies.Database;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup;

public class ServerBackupRunnerTests : ClusterTestBase
{
    public ServerBackupRunnerTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task ScheduledFullBackupMustBeFullAndNotRepeatEveryTick()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var server = GetNewServer();
        using var store = GetDocumentStore(new Options { Server = server });
        await StoreDocument(store);

        var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *", incrementalBackupFrequency: "0 0 1 1 *");

        await WaitForSecondOfMinuteBelow(40);
        var taskId = await Backup.UpdateConfigAndRunBackupAsync(server, config, store, isFullBackup: true);
        var manualDoneUtc = DateTime.UtcNow;

        Assert.True(await WaitForValueAsync(() => Task.FromResult(Decisions(server, store.Database, taskId)
                .Any(d => d.Category == BackupDecisionCategory.Completed && d.LastTime > manualDoneUtc)), true, timeout: 90_000, interval: 250),
            "precondition: the scheduler should run the next backup at the minute boundary");

        await WaitForRunnerRunsAsync(server, store.Database, taskId, runs: 15);

        var decisions = Decisions(server, store.Database, taskId);
        var scheduledStarts = decisions.Where(d => d.Category == BackupDecisionCategory.Started && d.LastTime > manualDoneUtc).OrderBy(d => d.LastTime).ToList();
        var status = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
        var files = Directory.GetFiles(backupPath, "*", SearchOption.AllDirectories).GroupBy(Path.GetExtension).Select(g => $"{g.Key}={g.Count()}");

        Assert.True(status.IsFull && scheduledStarts.Count == 1,
            $"Expected one scheduled FULL backup at the minute boundary. Got {scheduledStarts.Count} scheduled start(s) within ~15s " +
            $"(first at {scheduledStarts.FirstOrDefault()?.LastTime:O}); last status IsFull={status.IsFull}, " +
            $"LastFullBackup={status.LastFullBackup:O}, LastIncrementalBackup={status.LastIncrementalBackup:O}. " +
            $"Files: {string.Join(", ", files)}{Environment.NewLine}{Dump(decisions)}");
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task ServerWideBackupAddedWhileDatabaseIsIdleMustRun()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var server = GetNewServer(new ServerCreationOptions
        {
            RunInMemory = false,
            CustomSettings = IdleSettings()
        });
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
        await StoreDocument(store);

        server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().SkipIncreasingLastWorkTimeBasedOnDatabaseSize = true;
        Assert.True(WaitForValue(() => server.ServerStore.IdleDatabases.ContainsKey(store.Database), true, timeout: 90_000, interval: 500),
            "precondition: database should become idle");

        var put = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(new ServerWideBackupConfiguration
        {
            FullBackupFrequency = "* * * * *",
            LocalSettings = new LocalSettings { FolderPath = backupPath }
        }));
        await server.ServerStore.Cluster.WaitForIndexNotification(put.RaftCommandIndex);

        var taskId = GetSingleBackupTaskId(server, store.Database);
        var responsibleNode = Backup.WaitForResponsibleNodeUpdate(server.ServerStore, store.Database, taskId);
        Assert.Equal(server.ServerStore.NodeTag, responsibleNode);
        Assert.True(server.ServerStore.IdleDatabases.ContainsKey(store.Database), "precondition: database should still be idle");

        var backedUp = await WaitForValueAsync(() => Task.FromResult(GetClusterBackupStatus(server, store.Database, taskId)?.LastFullBackup != null),
            true, timeout: 130_000, interval: 1000);

        var tracked = server.ServerStore.ServerBackupRunner.GetDatabaseStateByTaskId(store.Database, taskId) != null;
        Assert.True(backedUp,
            $"Server-wide backup task {taskId} (every minute, responsible node '{responsibleNode}' = this node) never ran for idle database '{store.Database}' within 130s. " +
            $"Runner tracks the task: {tracked}. Database still idle: {server.ServerStore.IdleDatabases.ContainsKey(store.Database)}.");
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupNowMustReportStartFailure()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var server = GetNewServer(new ServerCreationOptions { RunInMemory = false });
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
        await StoreDocument(store);
        var taskId = await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(backupPath), store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(server, store);
        await File.WriteAllTextAsync(database.Configuration.Core.DataDirectory.Combine("disable.tasks.marker").FullPath, string.Empty);
        Assert.True(server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database), "precondition: unload database");
        database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
        Assert.True(database.DisableOngoingTasks, "precondition: disable.tasks.marker should disable ongoing tasks");

        Operation<StartBackupOperationResult> result = null;
        var error = await Record.ExceptionAsync(async () => result = await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: true, taskId)));

        OperationState operationState = null;
        if (result != null)
            operationState = await store.Maintenance.SendAsync(new GetOperationStateOperation(result.Id));

        Assert.True(error != null,
            $"Backup Now on a database with disable.tasks.marker did not fail; it returned operation id {result?.Id}. " +
            $"Operation registered on the server: {operationState != null}.{Environment.NewLine}{Dump(Decisions(server, store.Database, taskId))}");
        Assert.Contains("disabled via marker file", error.Message);
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task DisabledDatabaseMustNotGetFailedBackups()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var server = GetNewServer(new ServerCreationOptions
        {
            RunInMemory = false
        });
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
        await StoreDocument(store);

        await WaitForSecondOfMinuteBelow(40);
        var taskId = await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *"), store);

        await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, disable: true));
        Assert.False(WaitForValue(() => server.ServerStore.DatabasesLandlord.TryGetDatabaseIfLoaded(store.Database, out _), false, timeout: 15_000, interval: 100),
            "precondition: disabled database should be unloaded");
        var disabledAtUtc = DateTime.UtcNow;
        var dueUtc = disabledAtUtc.AddTicks(-(disabledAtUtc.Ticks % TimeSpan.TicksPerMinute)).AddMinutes(1);

        Assert.True(await WaitForValueAsync(() => Task.FromResult(Decisions(server, store.Database, taskId)
                .Any(d => d.Detail == DatabaseStatePolicy.Instance.Name && d.LastTime >= dueUtc)), true, timeout: 90_000, interval: 250),
            $"precondition: the runner should check the backup of the disabled database once it is due.{Environment.NewLine}{Dump(Decisions(server, store.Database, taskId))}");

        var afterDisable = Decisions(server, store.Database, taskId).Where(d => d.LastTime >= disabledAtUtc).ToList();
        var attempts = afterDisable.Where(d => d.Category is BackupDecisionCategory.Started or BackupDecisionCategory.Failed).ToList();
        var localStatus = server.ServerStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(store.Database, taskId);

        Assert.True(attempts.Count == 0,
            $"The runner tried to back up disabled database '{store.Database}' ({attempts.Count} Started/Failed decisions after it was disabled). " +
            $"Local status error: {localStatus?.Error?.Exception?.Split(Environment.NewLine).FirstOrDefault() ?? "none"}{Environment.NewLine}{Dump(afterDisable)}");
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupNowWhileScheduledBackupIsStartingMustNotBeSilentlyDropped()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var server = GetNewServer(new ServerCreationOptions
        {
            RunInMemory = false
        });
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
        await StoreDocument(store);
        var taskId = await Backup.UpdateConfigAndRunBackupAsync(server, Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *"), store);

        var runner = server.ServerStore.ServerBackupRunner;
        var landlord = server.ServerStore.DatabasesLandlord;
        var state = await WaitForStateAsync(runner, store.Database, taskId);
        Assert.NotNull(state);

        var database = await Databases.GetDocumentDatabaseInstanceFor(server, store);
        await WaitUntilNextMinuteBoundary(extra: TimeSpan.FromSeconds(10));
        Assert.False(WaitForValue(() => state.Running.IsRaised(), false, timeout: 60_000, interval: 100), "precondition: no backup is running");
        Assert.True(landlord.UnloadDirectly(store.Database), "precondition: unload database");

        var drained = 0;
        while (landlord._databaseSemaphore.Wait(0))
            drained++;
        var originalLoadTimeout = landlord._concurrentDatabaseLoadTimeout;
        landlord._concurrentDatabaseLoadTimeout = TimeSpan.FromMinutes(2);
        var released = false;

        try
        {
            const long backupNowOperationId = 1_000_002;

            Assert.True(WaitForValue(() => state.Running.IsRaised(), true, timeout: 75_000, interval: 50), "precondition: the scheduled backup is starting");
            Assert.Null(state.RunningTask);

            var error = Record.Exception(() => runner.StartBackupTask(database, taskId, isFullBackup: true, backupNowOperationId));

            Release();

            Assert.False(WaitForValue(() => state.Running.IsRaised(), false, timeout: 60_000, interval: 100), "the scheduled backup should finish");

            Assert.True(error is BackupAlreadyRunningException,
                $"Backup Now while a scheduled backup was starting returned operation id {backupNowOperationId} without throwing (error: {error?.GetType().Name ?? "none"}).");
        }
        finally
        {
            Release();
        }

        void Release()
        {
            if (released)
                return;

            released = true;
            if (drained > 0)
                landlord._databaseSemaphore.Release(drained);
            landlord._concurrentDatabaseLoadTimeout = originalLoadTimeout;
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
    public async Task ShardsOfTheSameDatabaseMustNotBlockEachOtherOnTheConcurrencyLimit()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Backup.MaxNumberOfConcurrentBackups)] = "1"
            }
        });
        using var store = Sharding.GetDocumentStore(new Options { Server = server });
        await StoreDocument(store);

        var shardNames = new List<string>();
        await foreach (var shard in Sharding.GetShardsDocumentDatabaseInstancesFor(store, new List<RavenServer> { server }))
            shardNames.Add(shard.Name);
        Assert.True(shardNames.Count > 1, "precondition: more than one shard on this node");

        var runner = server.ServerStore.ServerBackupRunner;
        var holds = new List<ManualResetEvent>();
        foreach (var shardName in shardNames)
        {
            var hold = new ManualResetEvent(false);
            holds.Add(hold);
            runner.ForTestingPurposesOnly().DatabaseTestingStuffInternals[shardName] = new ServerBackupRunner.TestingStuffInternal { HoldBackupFromFinishing = hold };
        }

        try
        {
            var startUtc = DateTime.UtcNow;
            var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *")));

            Assert.True(await WaitForValueAsync(() => Task.FromResult(StartedShards() > 0), true, timeout: 90_000, interval: 250),
                "precondition: at least one shard backup should start at the minute boundary");

            await WaitForValueAsync(() => Task.FromResult(StartedShards() == shardNames.Count), true, timeout: 30_000, interval: 250);

            var started = StartedShards();
            var capRejections = shardNames.Sum(name => Decisions(server, name, result.TaskId).Where(d => d.Detail == "ConcurrentBackup" && d.LastTime >= startUtc).Sum(d => d.Count));

            Assert.True(started == shardNames.Count,
                $"Only {started} of {shardNames.Count} shards of '{store.Database}' started; the server log has {capRejections} 'ConcurrentBackup' rejections. " +
                $"Running backups per counter: {server.ServerStore.ConcurrentBackupsCounter.CurrentNumberOfRunningBackups} (limit {server.ServerStore.ConcurrentBackupsCounter.MaxNumberOfConcurrentBackups}).");

            int StartedShards() => shardNames.Count(name => Decisions(server, name, result.TaskId).Any(d => d.Category == BackupDecisionCategory.Started && d.LastTime >= startUtc));
        }
        finally
        {
            foreach (var hold in holds)
                hold.Set();
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupDisabledWhileDatabaseIsIdleMustNotRunAfterWakeUp()
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");
        using var server = GetNewServer(new ServerCreationOptions
        {
            RunInMemory = false,
            CustomSettings = IdleSettings(keepIdleDatabaseLoadedBeforeBackupInMin: 0)
        });
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
        await StoreDocument(store);

        var put = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(new ServerWideBackupConfiguration
        {
            FullBackupFrequency = "* * * * *",
            LocalSettings = new LocalSettings { FolderPath = backupPath }
        }));
        await server.ServerStore.Cluster.WaitForIndexNotification(put.RaftCommandIndex);
        var taskId = GetSingleBackupTaskId(server, store.Database);
        Backup.WaitForResponsibleNodeUpdate(server.ServerStore, store.Database, taskId);

        var runner = server.ServerStore.ServerBackupRunner;
        Assert.True(WaitForValue(() => runner.GetDatabaseStateByTaskId(store.Database, taskId) != null, true), "precondition: runner tracks the task");

        server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().SkipIncreasingLastWorkTimeBasedOnDatabaseSize = true;

        for (var attempt = 0; ; attempt++)
        {
            Assert.True(WaitForValue(() => server.ServerStore.IdleDatabases.ContainsKey(store.Database), true, timeout: 90_000, interval: 250),
                "precondition: database should become idle");

            if (DateTime.Now.Second <= 40)
                break;

            Assert.True(attempt < 3, "precondition: could not catch the idle window");
            await WaitUntilNextMinuteBoundary(extra: TimeSpan.FromSeconds(2));
        }

        var serverWide = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(put.Name));
        serverWide.Disabled = true;
        var disable = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(serverWide));
        await server.ServerStore.Cluster.WaitForIndexNotification(disable.RaftCommandIndex);
        var disabledAtUtc = DateTime.UtcNow;

        Assert.True(GetSingleBackupConfiguration(server, store.Database).Disabled, "precondition: the task is disabled in the database record");
        Assert.True(server.ServerStore.IdleDatabases.ContainsKey(store.Database), "precondition: database is still idle when the task is disabled");
        var runnerStillSeesEnabled = runner.GetDatabaseStateByTaskId(store.Database, taskId).Configuration.Disabled == false;

        await WaitUntilNextMinuteBoundary(extra: TimeSpan.FromSeconds(20));

        var afterDisable = Decisions(server, store.Database, taskId).Where(d => d.LastTime >= disabledAtUtc).ToList();
        var started = afterDisable.Count(d => d.Category == BackupDecisionCategory.Started);

        Assert.True(started == 0,
            $"The runner started {started} backup(s) of task {taskId} after it was disabled at {disabledAtUtc:O} while the database was idle " +
            $"(runner configuration still enabled right after the disable: {runnerStillSeesEnabled}).{Environment.NewLine}{Dump(afterDisable)}");
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task DisableAndEnableWithinOneTickMustNotDuplicateQueueEntry()
    {
        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Backup.BackupRunnerFrequency)] = "3600"
            }
        });
        using var store = GetDocumentStore(new Options { Server = server });

        var config = Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder"));
        var taskId = await Backup.UpdateConfigAsync(server, config, store);

        var runner = server.ServerStore.ServerBackupRunner;
        var state = await WaitForStateAsync(runner, store.Database, taskId);
        Assert.NotNull(state);
        Assert.Equal(1, CountInQueue(runner, state));

        config.TaskId = taskId;
        config.Disabled = true;
        await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
        Assert.True(WaitForValue(() => state.Stale.IsRaised(), true), "precondition: disabled task is stale");

        config.Disabled = false;
        await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
        Assert.False(WaitForValue(() => state.Stale.IsRaised(), false, timeout: 30_000), "precondition: re-enabled task is not stale");

        var count = CountInQueue(runner, state);
        Assert.True(count == 1,
            $"Task {taskId} appears {count} times in the runner queue after disable+enable within one tick.");
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task OneTimeBackupMustRespectConcurrentBackupsLimit()
    {
        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Backup.MaxNumberOfConcurrentBackups)] = "1"
            }
        });
        using var store1 = GetDocumentStore(new Options { Server = server });
        using var store2 = GetDocumentStore(new Options { Server = server });
        await StoreDocument(store1);
        await StoreDocument(store2);

        var taskId = await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder1")), store1);

        var runner = server.ServerStore.ServerBackupRunner;
        var counter = server.ServerStore.ConcurrentBackupsCounter;
        var hold = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        runner.ForTestingPurposesOnly().DatabaseTestingStuffInternals[store1.Database] = new ServerBackupRunner.TestingStuffInternal { OnBackupTaskRunHoldBackupExecution = hold };

        try
        {
            var database1 = await Databases.GetDocumentDatabaseInstanceFor(server, store1);
            runner.StartBackupTask(database1, taskId, isFullBackup: true, database1.Operations.GetNextOperationId());
            Assert.Equal(1, counter.MaxNumberOfConcurrentBackups);
            Assert.Equal(1, WaitForValue(() => counter.CurrentNumberOfRunningBackups, 1));

            var error = await Record.ExceptionAsync(async () =>
            {
                var operation = await store2.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings { FolderPath = NewDataPath(suffix: "BackupFolder2") }
                }));
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            });

            Assert.True(error != null,
                $"A one-time backup of '{store2.Database}' completed while '{store1.Database}' held the only concurrent backup slot (limit {counter.MaxNumberOfConcurrentBackups}).");
            Assert.Contains("maximum number of concurrent backup tasks", error.Message);
        }
        finally
        {
            hold.TrySetResult(null);
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task BackupNowMustRespectServerHealthChecks()
    {
        using var server = GetNewServer(new ServerCreationOptions { IgnoreHealthChecksBeforeBackup = false });
        using var store = GetDocumentStore(new Options { Server = server });
        await StoreDocument(store);
        var taskId = await Backup.UpdateConfigAsync(server, Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder")), store);

        server.CpuCreditsBalance.BackgroundTasksAlertRaised.Raise();
        try
        {
            Operation<StartBackupOperationResult> result = null;
            var error = await Record.ExceptionAsync(async () => result = await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: true, taskId)));

            Assert.True(error != null,
                $"Backup Now started (operation id {result?.Id}) while the server's CPU credits were near exhaustion; the old runner rejected it with BackupDelayException.");
            Assert.Contains("CPU credits", error.Message);
        }
        finally
        {
            server.CpuCreditsBalance.BackgroundTasksAlertRaised.Lower();
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task DecisionLogMustNotRecordEveryTickForTasksThatAreNotDue()
    {
        using var server = GetNewServer();
        using var store = GetDocumentStore(new Options { Server = server });
        await StoreDocument(store);

        var taskId = await Backup.UpdateConfigAndRunBackupAsync(server, Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder")), store);
        var startUtc = DateTime.UtcNow;

        await WaitForRunnerRunsAsync(server, store.Database, taskId, runs: 20);

        var log = Decisions(server, store.Database, taskId);
        var notDue = log.Where(d => d.Category is BackupDecisionCategory.Waiting or BackupDecisionCategory.HeldBack && d.LastTime >= startUtc).ToList();
        var distinctMessages = notDue.Select(d => d.Reason).Distinct().Count();

        var bytesPerEntry = notDue.Count == 0 ? 0 : notDue.Average(d => 32 + 22 + 2 * (d.Reason?.Length ?? 0));
        var projectedKbPerTask = bytesPerEntry * ServerBackupRunner.MaxDecisionLogSize / 1024;
        var hoursUntilLifecycleEntriesAreEvicted = ServerBackupRunner.MaxDecisionLogSize * 60 / 3600.0;

        Assert.True(notDue.Count <= 1,
            $"{notDue.Count} 'not due' policy entries were added in 20s for a single task ({distinctMessages} distinct message(s)). " +
            $"~{bytesPerEntry:N0} bytes per entry, ~{projectedKbPerTask:N0} KB retained per task at the {ServerBackupRunner.MaxDecisionLogSize}-entry cap; " +
            $"at the default 60s frequency the Started/Completed entries are pushed out after ~{hoursUntilLifecycleEntriesAreEvicted:N1}h.");
    }

    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster)]
    public async Task RunnerMustNotTrackBackupTasksOfDatabasesNotHostedOnThisNode()
    {
        var (nodes, _) = await CreateRaftCluster(2, shouldRunInMemory: false);
        var hostingNode = nodes[0];
        var otherNode = nodes[1];
        var databaseName = GetDatabaseName();

        using var store = new DocumentStore { Urls = new[] { hostingNode.WebUrl }, Database = databaseName }.Initialize();
        await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(databaseName)
        {
            Topology = new DatabaseTopology { Members = new List<string> { hostingNode.ServerStore.NodeTag } }
        }));

        var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder"))));
        await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(result.RaftCommandIndex, nodes);

        var info = await DisposeServerAndWaitForFinishOfDisposalAsync(otherNode);
        var revived = await ReviveNodeAsync(info, new ServerCreationOptions
        {
            RunInMemory = false,
            DeletePrevious = false,
            RegisterForDisposal = true
        });

        await revived.ServerStore.Cluster.WaitForIndexNotification(result.RaftCommandIndex);

        var tracked = revived.ServerStore.ServerBackupRunner.GetDatabaseStateByTaskId(databaseName, result.TaskId);
        var log = tracked == null ? new List<BackupDecision>() : Decisions(revived, databaseName, result.TaskId);

        Assert.True(tracked == null,
            $"Node '{revived.ServerStore.NodeTag}' does not host '{databaseName}' but after restart its backup runner tracks task {result.TaskId} " +
            $"and logged {log.Count} decision(s):{Environment.NewLine}{Dump(log, 5)}");
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task IdleDatabaseMustNotBeReloadedForIncrementalBackupsWithoutChanges()
    {
        using var server = GetNewServer(new ServerCreationOptions
        {
            RunInMemory = false,
            CustomSettings = IdleSettings()
        });
        using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
        await StoreDocument(store);

        var config = Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "0 0 1 1 *", incrementalBackupFrequency: "* * * * *");
        var taskId = await Backup.UpdateConfigAndRunBackupAsync(server, config, store);

        var landlord = server.ServerStore.DatabasesLandlord;
        var reloads = new ConcurrentQueue<(DateTime Time, string Caller)>();
        landlord.ForTestingPurposesOnly().SkipIncreasingLastWorkTimeBasedOnDatabaseSize = true;
        landlord.ForTestingPurposesOnly().AfterDatabaseCreation = x =>
        {
            if (string.Equals(x.Database?.Name, store.Database, StringComparison.OrdinalIgnoreCase))
                reloads.Enqueue((DateTime.UtcNow, x.caller));
        };
        var startUtc = DateTime.UtcNow;
        var lastIncrementalBackupBefore = GetClusterBackupStatus(server, store.Database, taskId).LastIncrementalBackup;

        Assert.True(WaitForValue(() => server.ServerStore.IdleDatabases.ContainsKey(store.Database), true, timeout: 90_000, interval: 500),
            "precondition: database should become idle");

        Assert.True(await WaitForValueAsync(() => Task.FromResult(Decisions(server, store.Database, taskId)
                .Where(d => d.Detail == "NoChanges" && d.LastTime >= startUtc).Sum(d => d.Count) >= 2), true, timeout: 180_000, interval: 1000),
            "precondition: two incremental backups should be skipped because nothing changed");

        Assert.True(await WaitForValueAsync(() => Task.FromResult(GetClusterBackupStatus(server, store.Database, taskId).LastIncrementalBackup != lastIncrementalBackupBefore), true,
                timeout: 30_000, interval: 500),
            "the skipped incremental backup should be saved in the cluster backup status");

        var started = Decisions(server, store.Database, taskId).Count(d => d.Category == BackupDecisionCategory.Started && d.LastTime >= startUtc);
        Assert.True(reloads.IsEmpty,
            $"With no writes, database '{store.Database}' was unloaded and reloaded {reloads.Count} time(s) " +
            $"(at {string.Join(", ", reloads.Select(r => $"{r.Time:HH:mm:ss}"))}) to run {started} incremental backup(s).");
    }

    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster)]
    public async Task DecisionLogMustBeServedPerNodeAndWatchedLive()
    {
        var (nodes, leader) = await CreateRaftCluster(2, watcherCluster: true);
        using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 2 });
        await StoreDocument(store);
        var taskId = await Backup.UpdateConfigAsync(leader, Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder")), store);

        var other = nodes.Single(x => x != leader);
        Assert.True(WaitForValue(() => other.ServerStore.ServerBackupRunner.GetDatabaseStateByTaskId(store.Database, taskId) != null, true),
            "precondition: the other node tracks the task");
        Assert.True(WaitForValue(() => Decisions(other, store.Database, taskId).Count > 0, true, timeout: 30_000, interval: 250),
            "precondition: the other node logged a decision for the task");

        using (var client = new HttpClient(new HttpClientHandler { AutomaticDecompression = DecompressionMethods.All }))
        {
            var url = $"{leader.WebUrl}/admin/debug/periodic-backup/decision-log?nodeTag={other.ServerStore.NodeTag}&database={store.Database}&taskId={taskId}&take=10";

            using var response = await client.GetAsync(url);
            response.EnsureSuccessStatusCode();
            using var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
            var root = json.RootElement;

            Assert.Equal(other.ServerStore.NodeTag, root.GetProperty("NodeTag").GetString());
            Assert.Contains(store.Database, root.GetProperty("Databases").EnumerateArray().Select(x => x.GetString()));

            var entries = root.GetProperty("Entries").EnumerateArray().ToList();
            Assert.Contains(entries, x => x.GetProperty("Source").GetString() == nameof(BackupDecisionSource.Task));

            foreach (var entry in entries)
            {
                Assert.Equal(other.ServerStore.NodeTag, entry.GetProperty("NodeTag").GetString());

                if (entry.GetProperty("Source").GetString() != nameof(BackupDecisionSource.Task))
                    continue;

                Assert.Equal(store.Database, entry.GetProperty("Database").GetString());
                Assert.Equal(taskId, entry.GetProperty("TaskId").GetInt64());
            }
        }

        var runner = leader.ServerStore.ServerBackupRunner;
        var database = await Databases.GetDocumentDatabaseInstanceFor(leader, store);
        var state = await WaitForStateAsync(runner, store.Database, taskId);
        Assert.False(WaitForValue(() => state.Running.IsRaised(), false, timeout: 60_000, interval: 100), "precondition: no backup is running");

        using var webSocket = new ClientWebSocket();
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        await webSocket.ConnectAsync(new Uri($"{leader.WebUrl.Replace("http", "ws")}/admin/debug/periodic-backup/decision-log/watch"), cts.Token);
        await webSocket.SendAsync(Encoding.UTF8.GetBytes($"{{\"Databases\":[\"{store.Database}\"],\"Take\":10}}"), WebSocketMessageType.Text, true, cts.Token);

        using (var snapshot = await ReceiveJsonAsync(webSocket, cts.Token))
        {
            Assert.Equal("Snapshot", snapshot.RootElement.GetProperty("Type").GetString());
            Assert.Equal(leader.ServerStore.NodeTag, snapshot.RootElement.GetProperty("NodeTag").GetString());
            Assert.True(snapshot.RootElement.TryGetProperty("Summary", out _));
        }

        var hold = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        runner.ForTestingPurposesOnly().DatabaseTestingStuffInternals[store.Database] = new ServerBackupRunner.TestingStuffInternal { OnBackupTaskRunHoldBackupExecution = hold };

        try
        {
            var operationId = runner.StartBackupTask(database, taskId, isFullBackup: true, database.Operations.GetNextOperationId());
            Assert.True(WaitForValue(() => state.RunningTask != null, true, timeout: 15_000, interval: 50), "precondition: the backup is running");

            await database.Operations.KillOperationAsync(operationId, CancellationToken.None);
        }
        finally
        {
            hold.TrySetResult(null);
        }

        var sawStarted = false;
        string cancelDetail = null;
        while (cancelDetail == null)
        {
            using var update = await ReceiveJsonAsync(webSocket, cts.Token);
            Assert.Equal("Update", update.RootElement.GetProperty("Type").GetString());

            foreach (var entry in update.RootElement.GetProperty("Entries").EnumerateArray())
            {
                var entryTaskId = entry.GetProperty("TaskId");
                if (entryTaskId.ValueKind != JsonValueKind.Number || entryTaskId.GetInt64() != taskId)
                    continue;

                var category = entry.GetProperty("Category").GetString();
                if (category == nameof(BackupDecisionCategory.Started))
                    sawStarted = true;
                else if (category == nameof(BackupDecisionCategory.Cancelled))
                    cancelDetail = entry.GetProperty("Detail").GetString();
            }
        }

        Assert.True(sawStarted, "The watch should send the Started decision of the backup");
        Assert.Equal(ServerBackupRunner.CancelReasonUser, cancelDetail);
    }

    [RavenTheory(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task IdleDatabasesMustSaveSkippedIncrementalBackupsInBatchesAndBackUpCompareExchangeChanges(bool mixedVersionCluster)
    {
        var settings = new Dictionary<string, string>(IdleSettings())
        {
            [RavenConfiguration.GetKey(x => x.Backup.MaxStatusUpdatesPerBatch)] = "2"
        };

        if (mixedVersionCluster)
            settings[RavenConfiguration.GetKey(x => x.Cluster.MaximalAllowedClusterVersion)] = (ClusterCommandsVersionManager.ClusterCommandsVersions[nameof(UpdatePeriodicBackupStatusBatchCommand)] - 1).ToString();

        var (nodes, server) = await CreateRaftCluster(2, shouldRunInMemory: false, customSettings: settings);
        var other = nodes.Single(x => x != server);

        var batchSupported = mixedVersionCluster == false;
        Assert.True(WaitForValue(() => server.ServerStore.Engine.CommandsVersionManager.CanPutCommand(nameof(UpdatePeriodicBackupStatusBatchCommand)), batchSupported) == batchSupported,
            $"precondition: the batch command should {(mixedVersionCluster ? "not " : string.Empty)}be supported by the cluster");

        var stores = new List<(DocumentStore Store, long TaskId)>();

        try
        {
            for (var i = 0; i < 3; i++)
            {
                var store = GetDocumentStore(new Options
                {
                    Server = server,
                    RunInMemory = false,
                    ReplicationFactor = 1,
                    ModifyDatabaseRecord = record => record.Topology = new DatabaseTopology { Members = new List<string> { server.ServerStore.NodeTag } }
                });
                await StoreDocument(store);

                var config = Backup.CreateBackupConfiguration(NewDataPath(suffix: $"BackupFolder{i}"), fullBackupFrequency: "0 0 1 1 *", incrementalBackupFrequency: "* * * * *");
                stores.Add((store, await Backup.UpdateConfigAndRunBackupAsync(server, config, store)));
            }

            var lastIncrementalBackups = stores.ToDictionary(x => x.Store.Database, x => GetClusterBackupStatus(server, x.Store.Database, x.TaskId).LastIncrementalBackup);

            server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().SkipIncreasingLastWorkTimeBasedOnDatabaseSize = true;
            Assert.True(WaitForValue(() => stores.All(x => server.ServerStore.IdleDatabases.ContainsKey(x.Store.Database)), true, timeout: 90_000, interval: 500),
                "precondition: all databases should become idle");

            foreach (var (store, taskId) in stores)
            {
                Assert.True(await WaitForValueAsync(() => Task.FromResult(GetClusterBackupStatus(server, store.Database, taskId).LastIncrementalBackup != lastIncrementalBackups[store.Database]), true,
                        timeout: 150_000, interval: 1000),
                    $"The skipped incremental backup of '{store.Database}' wasn't saved.{Environment.NewLine}{Dump(Decisions(server, store.Database, taskId))}");

                var status = GetClusterBackupStatus(server, store.Database, taskId);
                Assert.Null(status.Error);
                Assert.Equal(server.ServerStore.NodeTag, status.NodeTag);
                Assert.True(server.ServerStore.IdleDatabases.ContainsKey(store.Database), $"'{store.Database}' should still be idle");

                Assert.True(await WaitForValueAsync(() => Task.FromResult(GetClusterBackupStatus(other, store.Database, taskId).LastIncrementalBackup == status.LastIncrementalBackup), true),
                    $"The skipped incremental backup status of '{store.Database}' should reach node '{other.ServerStore.NodeTag}'");
            }

            var (changedStore, changedTaskId) = stores[0];

            await WaitForSecondOfMinuteBelow(40);
            var putUtc = DateTime.UtcNow;
            var put = await changedStore.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("users/lock", new User { Name = "Grisha" }, 0));
            Assert.True(put.Successful, "precondition: compare exchange value should be saved");

            BackupDecision completed = null;
            Assert.True(await WaitForValueAsync(() =>
                {
                    completed = Decisions(server, changedStore.Database, changedTaskId).FirstOrDefault(d => d.Category == BackupDecisionCategory.Completed && d.LastTime >= putUtc);
                    return Task.FromResult(completed != null);
                }, true, timeout: 120_000, interval: 1000),
                $"The incremental backup after a compare exchange change was skipped.{Environment.NewLine}{Dump(Decisions(server, changedStore.Database, changedTaskId))}");

            var skipped = Decisions(server, changedStore.Database, changedTaskId).Where(d => d.Detail == "NoChanges" && d.LastTime >= putUtc && d.LastTime <= completed.LastTime).ToList();
            Assert.True(skipped.Count == 0, $"The incremental backup after a compare exchange change was skipped.{Environment.NewLine}{Dump(skipped)}");
        }
        finally
        {
            foreach (var (store, _) in stores)
                store.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
    public async Task IdleShardsMustSaveSkippedIncrementalBackupsWithoutBeingReloaded()
    {
        using var server = GetNewServer(new ServerCreationOptions
        {
            RunInMemory = false,
            CustomSettings = IdleSettings()
        });
        using var store = Sharding.GetDocumentStore(new Options { Server = server, RunInMemory = false });
        await StoreDocument(store);

        var config = Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "0 0 1 1 *", incrementalBackupFrequency: "* * * * *");
        var (taskId, backupOperation) = await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(server, store, config, isFullBackup: true);
        await backupOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

        var shardNames = new List<string>();
        await foreach (var shard in Sharding.GetShardsDocumentDatabaseInstancesFor(store, new List<RavenServer> { server }))
            shardNames.Add(shard.Name);
        Assert.True(shardNames.Count > 1, "precondition: more than one shard on this node");

        Assert.True(await WaitForValueAsync(() => Task.FromResult(shardNames.All(x => GetClusterBackupStatus(server, x, taskId)?.LastFullBackup != null)), true),
            "precondition: every shard should have a full backup");

        var lastIncrementalBackups = shardNames.ToDictionary(x => x, x => GetClusterBackupStatus(server, x, taskId).LastIncrementalBackup);

        var landlord = server.ServerStore.DatabasesLandlord;
        var reloads = new ConcurrentQueue<string>();
        landlord.ForTestingPurposesOnly().SkipIncreasingLastWorkTimeBasedOnDatabaseSize = true;
        landlord.ForTestingPurposesOnly().AfterDatabaseCreation = x =>
        {
            if (x.Database != null && shardNames.Contains(x.Database.Name, StringComparer.OrdinalIgnoreCase))
                reloads.Enqueue(x.Database.Name);
        };

        Assert.True(WaitForValue(() => shardNames.All(x => server.ServerStore.IdleDatabases.ContainsKey(x)), true, timeout: 90_000, interval: 500),
            "precondition: all shards should become idle");

        foreach (var shardName in shardNames)
        {
            Assert.True(await WaitForValueAsync(() => Task.FromResult(GetClusterBackupStatus(server, shardName, taskId).LastIncrementalBackup != lastIncrementalBackups[shardName]), true,
                    timeout: 150_000, interval: 1000),
                $"The skipped incremental backup of shard '{shardName}' wasn't saved.{Environment.NewLine}{Dump(Decisions(server, shardName, taskId))}");
        }

        Assert.True(reloads.IsEmpty, $"Idle shards were reloaded to skip their incremental backups: {string.Join(", ", reloads)}");
    }

    private async Task WaitForRunnerRunsAsync(RavenServer server, string database, long taskId, int runs)
    {
        var before = Decisions(server, database, taskId).Sum(d => d.Count);

        Assert.True(await WaitForValueAsync(() => Task.FromResult(Decisions(server, database, taskId).Sum(d => d.Count) - before >= runs), true,
                timeout: runs * 1000 + 60_000, interval: 250),
            $"precondition: the backup runner should check task {taskId} {runs} times");
    }

    private static async Task<JsonDocument> ReceiveJsonAsync(ClientWebSocket socket, CancellationToken token)
    {
        var buffer = new byte[64 * 1024];
        using var ms = new MemoryStream();

        while (true)
        {
            var result = await socket.ReceiveAsync(new ArraySegment<byte>(buffer), token);
            ms.Write(buffer, 0, result.Count);

            if (result.EndOfMessage)
                break;
        }

        ms.Position = 0;
        return await JsonDocument.ParseAsync(ms, cancellationToken: token);
    }

    private static Dictionary<string, string> IdleSettings(int keepIdleDatabaseLoadedBeforeBackupInMin = 5)
    {
        return new Dictionary<string, string>
        {
            [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
            [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
            [RavenConfiguration.GetKey(x => x.Backup.KeepIdleDatabaseLoadedBeforeBackup)] = keepIdleDatabaseLoadedBeforeBackupInMin.ToString()
        };
    }

    private static async Task StoreDocument(IDocumentStore store)
    {
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "Grisha" }, "users/1");
            await session.SaveChangesAsync();
        }
    }

    private static List<BackupDecision> Decisions(RavenServer server, string database, long taskId)
    {
        return server.ServerStore.ServerBackupRunner.GetDecisionLogFor(database, taskId) ?? new List<BackupDecision>();
    }

    private static string Dump(IEnumerable<BackupDecision> decisions, int take = 15)
    {
        return string.Join(Environment.NewLine, decisions.Take(take).Select(d => $"{d.LastTime:HH:mm:ss.fff} x{d.Count} {d.Category} {d.Detail} {d.Reason}"));
    }

    private async Task<DatabaseBackupState> WaitForStateAsync(ServerBackupRunner runner, string database, long taskId)
    {
        DatabaseBackupState state = null;

        Assert.True(await WaitForValueAsync(() => Task.FromResult((state = runner.GetDatabaseStateByTaskId(database, taskId)) != null), true, timeout: 15_000, interval: 100),
            $"precondition: runner should track backup task {taskId} of '{database}'");

        return state;
    }

    private static int CountInQueue(ServerBackupRunner runner, DatabaseBackupState state)
    {
        return runner.ForTestingPurposesOnly().CountInQueue(state);
    }

    private static PeriodicBackupConfiguration GetSingleBackupConfiguration(RavenServer server, string database)
    {
        using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        using (var record = server.ServerStore.Cluster.ReadRawDatabaseRecord(context, database))
            return record.PeriodicBackups.Single();
    }

    private static long GetSingleBackupTaskId(RavenServer server, string database) => GetSingleBackupConfiguration(server, database).TaskId;

    private static PeriodicBackupStatus GetClusterBackupStatus(RavenServer server, string database, long taskId)
    {
        using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
            return BackupUtils.GetBackupStatusFromCluster(context, database, taskId);
    }

    private static async Task WaitForSecondOfMinuteBelow(int second)
    {
        while (DateTime.Now.Second >= second)
            await Task.Delay(250);
    }

    private static async Task WaitUntilNextMinuteBoundary(TimeSpan extra)
    {
        var now = DateTime.Now;
        var nextMinute = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, now.Kind).AddMinutes(1);
        await Task.Delay(nextMinute - now + extra);
    }
}
