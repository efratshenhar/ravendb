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

    public string Name => "ConcurrentBackup";

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, DatabaseBackupState backupState, DateTime now, out string reason)
    {
        if (_concurrentBackupsCounter.CanRunBackup(backupState.OriginalDatabaseName) == false)
        {
            reason = $"Cannot start backup {backupState} because the maximum number of concurrent backups ({_concurrentBackupsCounter.MaxNumberOfConcurrentBackups}) is reached.";
            return false;
        }

        reason = null;
        return true;
    }
}
