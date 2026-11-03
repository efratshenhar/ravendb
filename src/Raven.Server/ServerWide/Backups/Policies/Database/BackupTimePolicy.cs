using System;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

public class BackupTimePolicy : IDatabaseBackupPolicy
{
    public static readonly BackupTimePolicy Instance = new();

    private BackupTimePolicy()
    {

    }

    public string Name => "BackupTime";

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, DatabaseBackupState backupState, DateTime now, out string reason, out BackupDecisionCategory category)
    {
        category = BackupDecisionCategory.Waiting;

        if (backupState.NextBackup == null)
        {
            var backupStatus = BackupUtils.GetBackupStatusFromCluster(context, backupState.DatabaseName, backupState.Configuration.TaskId);
            backupState.NextBackup = BackupUtils.GetNextBackupDetails(new BackupUtils.NextBackupDetailsParameters
            {
                BackupStatus = backupStatus,
                Configuration = backupState.Configuration,
                NodeTag = serverStore.NodeTag
            });
        }

        if (backupState.NextBackup == null)
        {
            reason = "The next backup time can't be calculated.";
            category = BackupDecisionCategory.HeldBack;
            return false;
        }

        if (backupState.NextBackup.DateTime > now)
        {
            reason = $"The backup isn't due yet. Next backup at '{backupState.NextBackup.DateTime}'.";
            return false;
        }

        reason = null;
        return true;
    }
}
