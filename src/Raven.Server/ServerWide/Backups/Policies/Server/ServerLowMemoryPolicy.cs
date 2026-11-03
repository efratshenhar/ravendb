using System;
using Sparrow.LowMemory;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

public class ServerLowMemoryPolicy : IServerBackupPolicy
{
    public static bool Disabled;

    private const string Reason = "Cannot start any backup(s) because the server entered the low memory state.";

    private DateTime _heldUntil;

    public string Name => "LowMemory";

    public bool CanDoBackup(ServerStore serverStore, DateTime now, string databaseName, out string reason)
    {
        if (Disabled)
        {
            reason = null;
            return true;
        }

        if (LowMemoryNotification.Instance.LowMemoryState)
            _heldUntil = now + serverStore.Configuration.Backup.LowMemoryBackupDelay.AsTimeSpan;

        if (now < _heldUntil)
        {
            reason = $"{Reason} Held back until '{_heldUntil}'.";
            return false;
        }

        reason = null;
        return true;
    }
}
