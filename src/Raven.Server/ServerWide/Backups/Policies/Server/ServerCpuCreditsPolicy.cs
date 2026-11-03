using System;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

public class ServerCpuCreditsPolicy : IServerBackupPolicy
{
    public static bool Disabled;

    private const string Reason = "The server CPU credits are near exhaustion.";

    private DateTime _heldUntil;

    public string Name => "CpuCredits";

    public bool CanDoBackup(ServerStore serverStore, DateTime now, out string reason, out BackupDecisionCategory category)
    {
        category = BackupDecisionCategory.HeldBack;

        if (Disabled)
        {
            reason = null;
            return true;
        }

        if (serverStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised() && now >= _heldUntil)
            _heldUntil = now + serverStore.Configuration.Server.CpuCreditsExhaustionBackupDelay.AsTimeSpan;

        if (now < _heldUntil)
        {
            reason = $"{Reason} Backups are held for {serverStore.Configuration.Server.CpuCreditsExhaustionBackupDelay.AsTimeSpan} and then checked again.";
            return false;
        }

        reason = null;
        return true;
    }
}
