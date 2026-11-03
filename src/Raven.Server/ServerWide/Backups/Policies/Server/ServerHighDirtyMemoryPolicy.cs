using System;
using Sparrow.LowMemory;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

public class ServerHighDirtyMemoryPolicy : IServerBackupPolicy
{
    public static bool Disabled;

    private const string Reason = "The server has high dirty memory.";

    private DateTime _heldUntil;

    public string Name => "HighDirtyMemory";

    public bool CanDoBackup(ServerStore serverStore, DateTime now, out string reason, out BackupDecisionCategory category)
    {
        category = BackupDecisionCategory.HeldBack;

        if (Disabled)
        {
            reason = null;
            return true;
        }

        if (LowMemoryNotification.Instance.DirtyMemoryState.IsHighDirty && now >= _heldUntil)
            _heldUntil = now + serverStore.Configuration.Backup.LowMemoryBackupDelay.AsTimeSpan;

        if (now < _heldUntil)
        {
            reason = $"{Reason} Backups are held for {serverStore.Configuration.Backup.LowMemoryBackupDelay.AsTimeSpan} and then checked again.";
            return false;
        }

        reason = null;
        return true;
    }
}
