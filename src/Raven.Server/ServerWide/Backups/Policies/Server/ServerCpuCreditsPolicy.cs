using System;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

public class ServerCpuCreditsPolicy : IServerBackupPolicy
{
    public static bool Disabled;

    private const string Reason = "Cannot start any backup(s) because the server CPU credits were near exhaustion.";

    private DateTime _heldUntil;

    public string Name => "CpuCredits";

    public bool CanDoBackup(ServerStore serverStore, DateTime now, string databaseName, out string reason)
    {
        if (Disabled)
        {
            reason = null;
            return true;
        }

        if (serverStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised())
            _heldUntil = now + serverStore.Configuration.Server.CpuCreditsExhaustionBackupDelay.AsTimeSpan;

        if (now < _heldUntil)
        {
            reason = $"{Reason} Held back until '{_heldUntil}'.";
            return false;
        }

        reason = null;
        return true;
    }
}
