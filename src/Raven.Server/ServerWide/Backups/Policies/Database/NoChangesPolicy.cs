using System;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

public class NoChangesPolicy : IDatabaseBackupPolicy
{
    private readonly ServerBackupRunner _serverBackupRunner;

    public NoChangesPolicy([NotNull] ServerBackupRunner serverBackupRunner)
    {
        _serverBackupRunner = serverBackupRunner ?? throw new ArgumentNullException(nameof(serverBackupRunner));
    }

    public string Name => "NoChanges";

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, DatabaseBackupState backupState, DateTime now, out string reason)
    {
        if (_serverBackupRunner.CanSkipIncrementalBackup(context, backupState) == false)
        {
            reason = null;
            return true;
        }

        reason = $"Skipping the incremental backup {backupState} because the database is idle and nothing changed since the last backup.";
        return false;
    }
}
