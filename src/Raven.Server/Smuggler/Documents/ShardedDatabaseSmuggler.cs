using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Processors;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents
{
    public class ShardedDatabaseSmuggler : BaseSmuggler
    {
        private readonly TransactionOperationContext _tContext;
        private readonly List<DatabaseRecord.ShardRangeAssignment> _shardAllocation;
        private List<ISmugglerDestination> _destinations;

        public ShardedDatabaseSmuggler( ISmugglerSource source, 
            List<ISmugglerDestination> destination, 
            JsonOperationContext context, 
            TransactionOperationContext tContext,
            List<DatabaseRecord.ShardRangeAssignment> shardAllocation,
            SystemTime time, 
            DatabaseSmugglerOptionsServerSide options = null, 
            SmugglerResult result = null, 
            Action<IOperationProgress> onProgress = null, 
            CancellationToken token = default) : 
            base(source, time, context, options, result, onProgress, token)
        {
            _tContext = tContext;
            _shardAllocation = shardAllocation;
            _destinations = destination;

        }


        public override async Task<SmugglerResult> ExecuteAsync(bool ensureStepsProcessed = true, bool isLastFile = true)
        {
            var result = _result ?? new SmugglerResult();
            var desList = new List<IAsyncDisposable>();
            try
            {
                using (var initializeResult = await _source.InitializeAsync(_options, result))
                {
                    foreach (var des in _destinations)
                    {
                        desList.Add(des.InitializeAsync(_options, result, initializeResult.BuildNumber));
                    }

                    ModifyV41OperateOnTypes(initializeResult.BuildNumber, isLastFile);

                    var buildType = BuildVersion.Type(initializeResult.BuildNumber);
                    var currentType = await _source.GetNextTypeAsync();
                    while (currentType != DatabaseItemType.None)
                    {
                        await ProcessTypeAsync(currentType, result, buildType, ensureStepsProcessed);

                        currentType = await _source.GetNextTypeAsync();
                    }

                    if (ensureStepsProcessed)
                    {
                        EnsureProcessed(result);
                    }

                    return result;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
            finally
            {
                foreach (var des in desList)
                {
                    await des.DisposeAsync();
                }
            }
        }

        protected override Task<SmugglerProgressBase.DatabaseRecordProgress> ProcessDatabaseRecordAsync(SmugglerResult result)
        {
            throw new NotImplementedException();
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessDocumentsAsync(SmugglerResult result, BuildVersionType buildType)
        {
            var actionsList = new List<IDocumentActions>();
            var throwOnCollectionMismatchError = _options.OperateOnTypes.HasFlag(DatabaseItemType.Tombstones) == false;
            try
            {
                for (int i = 0; i < _destinations.Count; i++)
                {
                    actionsList.Add(_destinations[i].Documents(throwOnCollectionMismatchError));
                }
                await foreach (DocumentItem item in _source.GetDocumentsAsync(_options.Collections))
                {
                    var index = ShardHelper.GetShardIndexforDocument(_tContext, _shardAllocation, item.Document.Id);


                    await actionsList[index].WriteDocumentAsync(item, result.Documents);
                }
            }
            finally
            {
                foreach (var action in actionsList)
                {
                    await action.DisposeAsync();
                }
            }
            

            return result.Documents;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessRevisionDocumentsAsync(SmugglerResult result)
        {
            // result.RevisionDocuments.Start();
            // var actionsList = new List<IDocumentActions>();
            // try
            // {
            //     for (int i = 0; i < _destinations.Count; i++)
            //     {
            //         actionsList.Add(_destinations[i].RevisionDocuments());
            //     }
            //
            //     await foreach (var item in _source.GetRevisionDocumentsAsync(_options.Collections))
            //     {
            //         var index = ShardHelper.GetShardIndexforDocument(_tContext, _shardAllocation, item.Document.Id);
            //         await actionsList[index].WriteDocumentAsync(item, result.RevisionDocuments);
            //
            //         result.RevisionDocuments.LastEtag = item.Document.Etag;
            //     }
            //
            //
            //     return result.RevisionDocuments;
            // }
            // finally
            // {
            //     foreach (var action in actionsList)
            //     {
            //         await action.DisposeAsync();
            //     }
            // }
            throw new NotImplementedException();

        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessTombstonesAsync(SmugglerResult result, BuildVersionType buildType)
        {
            var actionsList = new List<IDocumentActions>();
           
            try
            {
                for (int i = 0; i < _destinations.Count; i++)
                {
                    actionsList.Add(_destinations[i].Tombstones());
                }

                await foreach (var tombstone in _source.GetTombstonesAsync(_options.Collections))
                {
                    var index = ShardHelper.GetShardIndexforDocument(_tContext, _shardAllocation, tombstone.LowerId);

                    await actionsList[index].WriteTombstoneAsync(tombstone, result.Tombstones);
                }
            }
            finally
            {
                foreach (var action in actionsList)
                {
                    await action.DisposeAsync();
                }
            }


            return result.Documents;
        }

        protected override async Task ProcessDocumentsWithDuplicateCollectionAsync(SmugglerResult result)
        {
            return;
        }

        protected override Task<SmugglerProgressBase.Counts> ProcessConflictsAsync(SmugglerResult result)
        {
            throw new NotImplementedException();
        }

        protected override Task<SmugglerProgressBase.Counts> ProcessIndexesAsync(SmugglerResult result)
        {
            throw new NotImplementedException();
        }

        protected override Task<SmugglerProgressBase.Counts> ProcessIdentitiesAsync(SmugglerResult result, BuildVersionType buildType)
        {
            throw new NotImplementedException();
        }

        protected override Task<SmugglerProgressBase.Counts> ProcessLegacyAttachmentsAsync(SmugglerResult result)
        {
            throw new NotImplementedException();
        }

        protected override Task<SmugglerProgressBase.Counts> ProcessLegacyDocumentDeletionsAsync(SmugglerResult result)
        {
            throw new NotImplementedException();
        }

        protected override Task<SmugglerProgressBase.Counts> ProcessLegacyAttachmentDeletionsAsync(SmugglerResult result)
        {
            throw new NotImplementedException();
        }

        protected override Task<SmugglerProgressBase.Counts> ProcessCompareExchangeAsync(SmugglerResult result)
        {
            throw new NotImplementedException();
        }

        protected override Task<SmugglerProgressBase.Counts> ProcessLegacyCountersAsync(SmugglerResult result)
        {
            throw new NotImplementedException();
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessCountersAsync(SmugglerResult result)
        {
            result.Counters.Start();
            
            var actionsList = new List<ICounterActions>();
            try
            {
                for (int i = 0; i < _destinations.Count; i++)
                {
                    actionsList.Add(_destinations[i].Counters(result));
                }
                var isFullBackup = _source.GetSourceType() == SmugglerSourceType.FullExport;
                await foreach (var counterGroup in _source.GetCounterValuesAsync(_options.Collections))//TODO - get without action
                {
                    var index = ShardHelper.GetShardIndexforDocument(_tContext, _shardAllocation, counterGroup.DocumentId);
                    await actionsList[index].WriteCounterAsync(counterGroup);
                }
                return result.Counters;
            }
            finally
            {
                foreach (var action in actionsList)
                {
                    await action.DisposeAsync();
                }
            }
        }

        protected override Task<SmugglerProgressBase.Counts> ProcessCompareExchangeTombstonesAsync(SmugglerResult result)
        {
            throw new NotImplementedException();
        }

        protected override Task<SmugglerProgressBase.Counts> ProcessSubscriptionsAsync(SmugglerResult result)
        {
            throw new NotImplementedException();
        }

        protected override Task<SmugglerProgressBase.Counts> ProcessReplicationHubCertificatesAsync(SmugglerResult result)
        {
            throw new NotImplementedException();
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessTimeSeriesAsync(SmugglerResult result)
        {
            result.TimeSeries.Start();
            var actionsList = new List<ITimeSeriesActions>();
            try
            {
                for (int i = 0; i < _destinations.Count; i++)
                {
                    actionsList.Add(_destinations[i].TimeSeries());
                }
                var isFullBackup = _source.GetSourceType() == SmugglerSourceType.FullExport;
                await foreach (var ts in _source.GetTimeSeriesAsync(_options.Collections))
                {
                    var index = ShardHelper.GetShardIndexforDocument(_tContext, _shardAllocation, ts.DocId);
                    await actionsList[index].WriteTimeSeriesAsync(ts);
                }
                return result.TimeSeries;
            }
            finally
            {
                foreach (var action in actionsList)
                {
                    await action.DisposeAsync();
                }
            }

        }
    }
}
