using System;
using JetBrains.Annotations;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

public class ConcurrentBackupPolicy : IDatabaseBackupPolicy
{
    private readonly ConcurrentBackupsCounter _concurrentBackupsCounter;

    public ConcurrentBackupPolicy([NotNull] ConcurrentBackupsCounter concurrentBackupsCounter)
    {
        _concurrentBackupsCounter = concurrentBackupsCounter ?? throw new ArgumentNullException(nameof(concurrentBackupsCounter));
    }

    public string Name => ServerBackupRunner.ConcurrentBackupDetail;

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, DatabaseBackupState backupState, DateTime now, out string reason, out BackupDecisionCategory category)
    {
        category = BackupDecisionCategory.Waiting;

        if (_concurrentBackupsCounter.CanRunBackup(backupState.OriginalDatabaseName) == false)
        {
            reason = $"The maximum number of concurrent backups ({_concurrentBackupsCounter.MaxNumberOfConcurrentBackups}) is reached.";
            return false;
        }

        reason = null;
        return true;
    }
}
