using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Exceptions;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Backups;

public enum BackupDecisionSource
{
    Server,
    Task
}

public enum BackupDecisionCategory
{
    Waiting,
    HeldBack,
    Started,
    Completed,
    Failed,
    Cancelled
}

public sealed class BackupDecision
{
    public long Id { get; init; }

    public DateTime FirstTime { get; init; }

    public DateTime LastTime { get; internal set; }

    public int Count { get; internal set; }

    public BackupDecisionCategory Category { get; init; }

    public string Detail { get; init; }

    public string Reason { get; init; }

    internal long Etag { get; set; }

    internal bool IsSameAs(BackupDecisionCategory category, string detail, string reason)
    {
        return Category == category &&
               string.Equals(Detail, detail, StringComparison.Ordinal) &&
               string.Equals(Reason, reason, StringComparison.Ordinal);
    }
}

public sealed class BackupDecisionLog
{
    private readonly List<BackupDecision> _decisions = new();

    private readonly int _maxSize;

    public BackupDecisionLog(int maxSize)
    {
        _maxSize = maxSize;
    }

    internal BackupDecision Add(BackupDecisionCategory category, string detail, string reason, DateTime time, Func<long> nextId, Func<long> nextEtag)
    {
        lock (_decisions)
        {
            if (_decisions.Count > 0 && _decisions[0].IsSameAs(category, detail, reason))
            {
                var latest = _decisions[0];
                latest.Count++;
                latest.LastTime = time;
                latest.Etag = nextEtag();

                return latest;
            }

            var decision = new BackupDecision
            {
                Id = nextId(),
                FirstTime = time,
                LastTime = time,
                Count = 1,
                Category = category,
                Detail = detail,
                Reason = reason,
                Etag = nextEtag()
            };

            _decisions.Insert(0, decision);

            if (_decisions.Count > _maxSize)
                _decisions.RemoveAt(_decisions.Count - 1);

            return decision;
        }
    }

    public List<BackupDecision> GetDecisions(long? afterEtag = null)
    {
        lock (_decisions)
        {
            return _decisions.TakeWhile(x => afterEtag == null || x.Etag > afterEtag).Select(Copy).ToList();
        }
    }

    public bool TryGetLatest(out BackupDecision decision)
    {
        lock (_decisions)
        {
            if (_decisions.Count == 0)
            {
                decision = null;
                return false;
            }

            decision = Copy(_decisions[0]);
            return true;
        }
    }

    private static BackupDecision Copy(BackupDecision decision)
    {
        return new BackupDecision
        {
            Id = decision.Id,
            FirstTime = decision.FirstTime,
            LastTime = decision.LastTime,
            Count = decision.Count,
            Category = decision.Category,
            Detail = decision.Detail,
            Reason = decision.Reason,
            Etag = decision.Etag
        };
    }
}

public sealed class BackupDecisionLogFilter
{
    public HashSet<string> Databases { get; set; }

    public HashSet<long> TaskIds { get; set; }

    public HashSet<BackupDecisionCategory> Categories { get; set; }

    public string Reason { get; set; }

    public DateTime? From { get; set; }

    public DateTime? To { get; set; }

    public int Take { get; set; }

    internal bool MatchesTask(string database, long taskId)
    {
        return (Databases is not { Count: > 0 } || Databases.Contains(database)) &&
               (TaskIds is not { Count: > 0 } || TaskIds.Contains(taskId));
    }

    public bool Matches(BackupDecisionLogEntry entry)
    {
        if (entry.Source == BackupDecisionSource.Task && MatchesTask(entry.Database, entry.TaskId.Value) == false)
            return false;

        if (Categories is { Count: > 0 } && Categories.Contains(entry.Category) == false)
            return false;

        if (string.IsNullOrEmpty(Reason) == false && (entry.Reason == null || entry.Reason.Contains(Reason, StringComparison.OrdinalIgnoreCase) == false))
            return false;

        if (From != null && entry.LastTime < From.Value)
            return false;

        if (To != null && entry.FirstTime > To.Value)
            return false;

        return true;
    }

    public static long ParseTaskId(string value)
    {
        if (long.TryParse(value, out var taskId) == false)
            throw new BadRequestException($"Invalid task id '{value}'. A task id must be a number.");

        return taskId;
    }

    public static BackupDecisionCategory ParseCategory(string value)
    {
        if (Enum.TryParse<BackupDecisionCategory>(value, ignoreCase: true, out var category) == false || Enum.IsDefined(category) == false)
            throw new BadRequestException($"Invalid category '{value}'. Valid categories are: {string.Join(", ", Enum.GetNames<BackupDecisionCategory>())}.");

        return category;
    }

    public static BackupDecisionLogFilter FromJson(BlittableJsonReaderObject json, int defaultTake, int maxTake)
    {
        var filter = new BackupDecisionLogFilter { Take = defaultTake };

        if (json.TryGet(nameof(Databases), out BlittableJsonReaderArray databases) && databases != null)
            filter.Databases = new HashSet<string>(databases.Select(x => x.ToString()), StringComparer.OrdinalIgnoreCase);

        if (json.TryGet(nameof(TaskIds), out BlittableJsonReaderArray taskIds) && taskIds != null)
            filter.TaskIds = taskIds.Select(x => ParseTaskId(x.ToString())).ToHashSet();

        if (json.TryGet(nameof(Categories), out BlittableJsonReaderArray categories) && categories != null)
            filter.Categories = categories.Select(x => ParseCategory(x.ToString())).ToHashSet();

        if (json.TryGet(nameof(Reason), out string reason))
            filter.Reason = reason;

        if (json.TryGet(nameof(From), out DateTime? from))
            filter.From = from;

        if (json.TryGet(nameof(To), out DateTime? to))
            filter.To = to;

        if (json.TryGet(nameof(Take), out int? take) && take != null)
            filter.Take = Math.Clamp(take.Value, 0, maxTake);

        return filter;
    }
}

public sealed class BackupDecisionLogDetails : IDynamicJson
{
    public string NodeTag { get; set; }

    public DateTime RunnerStartTime { get; set; }

    public BackupDecisionLogSummary Summary { get; set; }

    public List<BackupDecisionLogEntry> Entries { get; set; } = new();

    public int TotalResults { get; set; }

    public List<string> Databases { get; set; } = new();

    public List<BackupDecisionLogTask> Tasks { get; set; } = new();

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(NodeTag)] = NodeTag,
            [nameof(RunnerStartTime)] = RunnerStartTime,
            [nameof(Summary)] = Summary?.ToJson(),
            [nameof(Entries)] = new DynamicJsonArray(Entries.Select(x => x.ToJson())),
            [nameof(TotalResults)] = TotalResults,
            [nameof(Databases)] = new DynamicJsonArray(Databases),
            [nameof(Tasks)] = new DynamicJsonArray(Tasks.Select(x => x.ToJson()))
        };
    }
}

public sealed class BackupDecisionLogTask : IDynamicJson
{
    public string Database { get; set; }

    public long TaskId { get; set; }

    public string TaskName { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Database)] = Database,
            [nameof(TaskId)] = TaskId,
            [nameof(TaskName)] = TaskName
        };
    }
}

public sealed class BackupDecisionLogSummary : IDynamicJson
{
    public RunningSummary Running { get; set; } = new();

    public WaitingForScheduleSummary WaitingForSchedule { get; set; } = new();

    public HeldBackSummary HeldBack { get; set; } = new();

    public ServerBlockSummary ServerBlock { get; set; }

    public OutcomeSummary Failed { get; set; } = new();

    public OutcomeSummary Completed { get; set; } = new();

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Running)] = Running.ToJson(),
            [nameof(WaitingForSchedule)] = WaitingForSchedule.ToJson(),
            [nameof(HeldBack)] = HeldBack.ToJson(),
            [nameof(ServerBlock)] = ServerBlock?.ToJson(),
            [nameof(Failed)] = Failed.ToJson(),
            [nameof(Completed)] = Completed.ToJson()
        };
    }

    public sealed class RunningSummary
    {
        public int Current { get; set; }

        public int Max { get; set; }

        public int WaitingForSlot { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Current)] = Current,
                [nameof(Max)] = Max,
                [nameof(WaitingForSlot)] = WaitingForSlot
            };
        }
    }

    public sealed class WaitingForScheduleSummary
    {
        public int Count { get; set; }

        public DateTime? NextBackupUtc { get; set; }

        public string NextBackupDatabase { get; set; }

        public string NextBackupTaskName { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Count)] = Count,
                [nameof(NextBackupUtc)] = NextBackupUtc,
                [nameof(NextBackupDatabase)] = NextBackupDatabase,
                [nameof(NextBackupTaskName)] = NextBackupTaskName
            };
        }
    }

    public sealed class HeldBackSummary
    {
        public int Count { get; set; }

        public HeldBackTask Longest { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Count)] = Count,
                [nameof(Longest)] = Longest?.ToJson()
            };
        }
    }

    public sealed class HeldBackTask
    {
        public string Database { get; set; }

        public long TaskId { get; set; }

        public string TaskName { get; set; }

        public string Detail { get; set; }

        public string Reason { get; set; }

        public DateTime Since { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Database)] = Database,
                [nameof(TaskId)] = TaskId,
                [nameof(TaskName)] = TaskName,
                [nameof(Detail)] = Detail,
                [nameof(Reason)] = Reason,
                [nameof(Since)] = Since
            };
        }
    }

    public sealed class ServerBlockSummary
    {
        public string Detail { get; set; }

        public string Reason { get; set; }

        public DateTime Since { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Detail)] = Detail,
                [nameof(Reason)] = Reason,
                [nameof(Since)] = Since
            };
        }
    }

    public sealed class OutcomeSummary
    {
        public long Count { get; set; }

        public Outcome Latest { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Count)] = Count,
                [nameof(Latest)] = Latest?.ToJson()
            };
        }
    }

    public sealed class Outcome
    {
        public string Database { get; set; }

        public long TaskId { get; set; }

        public string TaskName { get; set; }

        public DateTime Time { get; set; }

        public string Error { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Database)] = Database,
                [nameof(TaskId)] = TaskId,
                [nameof(TaskName)] = TaskName,
                [nameof(Time)] = Time,
                [nameof(Error)] = Error
            };
        }
    }
}

public sealed class BackupDecisionLogEntry : IDynamicJson
{
    public long Id { get; set; }

    public DateTime FirstTime { get; set; }

    public DateTime LastTime { get; set; }

    public int Count { get; set; }

    public BackupDecisionCategory Category { get; set; }

    public string Detail { get; set; }

    public string Reason { get; set; }

    public BackupDecisionSource Source { get; set; }

    public string Database { get; set; }

    public int? Shard { get; set; }

    public long? TaskId { get; set; }

    public string TaskName { get; set; }

    public string NodeTag { get; set; }

    internal long Etag { get; set; }

    public static BackupDecisionLogEntry Create(BackupDecision decision, string nodeTag, DatabaseBackupState backupState = null)
    {
        var entry = new BackupDecisionLogEntry
        {
            Id = decision.Id,
            FirstTime = decision.FirstTime,
            LastTime = decision.LastTime,
            Count = decision.Count,
            Category = decision.Category,
            Detail = decision.Detail,
            Reason = decision.Reason,
            Source = backupState == null ? BackupDecisionSource.Server : BackupDecisionSource.Task,
            NodeTag = nodeTag,
            Etag = decision.Etag
        };

        if (backupState == null)
            return entry;

        entry.Database = backupState.OriginalDatabaseName;
        entry.Shard = ShardHelper.TryGetShardNumberFromDatabaseName(backupState.DatabaseName, out var shardNumber) ? shardNumber : null;
        entry.TaskId = backupState.Configuration.TaskId;
        entry.TaskName = backupState.Configuration.Name;

        return entry;
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Id)] = Id,
            [nameof(FirstTime)] = FirstTime,
            [nameof(LastTime)] = LastTime,
            [nameof(Count)] = Count,
            [nameof(Category)] = Category,
            [nameof(Detail)] = Detail,
            [nameof(Reason)] = Reason,
            [nameof(Source)] = Source,
            [nameof(Database)] = Database,
            [nameof(Shard)] = Shard,
            [nameof(TaskId)] = TaskId,
            [nameof(TaskName)] = TaskName,
            [nameof(NodeTag)] = NodeTag
        };
    }
}
