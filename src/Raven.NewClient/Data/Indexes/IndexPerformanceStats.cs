﻿namespace Raven.NewClient.Client.Data.Indexes
{
    public class IndexPerformanceStats
    {
        public string IndexName { get; set; }

        public int IndexId { get; set; }

        public IndexingPerformanceStats[] Performance { get; set; }
    }
}