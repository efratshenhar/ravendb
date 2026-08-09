import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type BackupHistoryResult = {
    BackupHistory: Raven.Server.Documents.PeriodicBackup.BackupHistory.BackupHistory;
};

class getBackupHistoryCommand extends commandBase {
    private readonly dbName: string;

    constructor(dbName: string) {
        super();
        this.dbName = dbName;
    }

    execute(): JQueryPromise<BackupHistoryResult> {
        const args = {
            database: this.dbName,
            includeIncrementals: true,
        };

        const url = endpoints.global.backupDatabase.periodicBackupHistory;

        return this.query<BackupHistoryResult>(url, args).fail((response: JQueryXHR) =>
            this.reportError("Failed to get the backup history", response.responseText, response.statusText)
        );
    }
}

export = getBackupHistoryCommand;
