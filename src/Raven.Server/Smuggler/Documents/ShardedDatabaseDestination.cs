using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Smuggler.Documents
{
    
    public class ShardedDatabaseDestination : ISmugglerDestination
    {
        private readonly List<Stream> _streamList;
        private List<GZipStream> _gzipStream;
        private readonly JsonOperationContext _context;
        private readonly TransactionOperationContext _tContext;
        private readonly ISmugglerSource _source;
        private List<AsyncBlittableJsonTextWriter> _writer;
        private DatabaseSmugglerOptionsServerSide _options;
        private Func<LazyStringValue, bool> _filterMetadataProperty;
        private readonly List<DatabaseRecord.ShardRangeAssignment> _shardAllocations;
        public List<IDisposable> ToDispose;


        public ShardedDatabaseDestination(List<Stream> streamList, JsonOperationContext context, TransactionOperationContext tContext, ISmugglerSource source, List<DatabaseRecord.ShardRangeAssignment> shardAllocations) 
        {
            _streamList = streamList;
            _context = context;
            _source = source;
            _tContext = tContext;
            _gzipStream = new List<GZipStream>();
            _writer = new List<AsyncBlittableJsonTextWriter>();
            _shardAllocations = shardAllocations;
        }

        public IAsyncDisposable InitializeAsync(DatabaseSmugglerOptionsServerSide options, SmugglerResult result, long buildVersion)
        {
            _options = options;

            SetupMetadataFilterMethod(_context);
            for (int i = 0; i < _streamList.Count; i++)
            {
                
                _gzipStream.Add(new GZipStream(_streamList[i], CompressionMode.Compress, leaveOpen: true));
                _writer.Add(new AsyncBlittableJsonTextWriter(_context, _gzipStream[i]));

                _writer[i].WriteStartObject();

                _writer[i].WritePropertyName("BuildVersion");
                _writer[i].WriteInteger(buildVersion);

                
            }



            return new AsyncDisposableAction(async () =>
            {
                for (int i = 0; i < _streamList.Count; i++)
                {
                    _writer[i].WriteEndObject();
                    await _writer[i].DisposeAsync();
                    await _gzipStream[i].DisposeAsync();
                }
            });


        }

        private void SetupMetadataFilterMethod(JsonOperationContext context)
        {
            var skipCountersMetadata = _options.OperateOnTypes.HasFlag(DatabaseItemType.CounterGroups) == false;
            var skipAttachmentsMetadata = _options.OperateOnTypes.HasFlag(DatabaseItemType.Attachments) == false;
            var skipTimeSeriesMetadata = _options.OperateOnTypes.HasFlag(DatabaseItemType.TimeSeries) == false;

            var flags = 0;
            if (skipCountersMetadata)
                flags += 1;
            if (skipAttachmentsMetadata)
                flags += 2;
            if (skipTimeSeriesMetadata)
                flags += 4;

            if (flags == 0)
                return;

            var counters = context.GetLazyString(Constants.Documents.Metadata.Counters);
            var attachments = context.GetLazyString(Constants.Documents.Metadata.Attachments);
            var timeSeries = context.GetLazyString(Constants.Documents.Metadata.TimeSeries);

            switch (flags)
            {
                case 1: // counters
                    _filterMetadataProperty = metadataProperty => metadataProperty.Equals(counters);
                    break;

                case 2: // attachments
                    _filterMetadataProperty = metadataProperty => metadataProperty.Equals(attachments);
                    break;

                case 3: // counters, attachments
                    _filterMetadataProperty = metadataProperty => metadataProperty.Equals(counters) || metadataProperty.Equals(attachments);
                    break;

                case 4: // timeseries
                    _filterMetadataProperty = metadataProperty => metadataProperty.Equals(timeSeries);
                    break;

                case 5: // counters, timeseries
                    _filterMetadataProperty = metadataProperty => metadataProperty.Equals(counters) || metadataProperty.Equals(timeSeries);
                    break;

                case 6: // attachments, timeseries
                    _filterMetadataProperty = metadataProperty => metadataProperty.Equals(attachments) || metadataProperty.Equals(timeSeries);
                    break;

                case 7: // counters, attachments, timeseries
                    _filterMetadataProperty = metadataProperty => metadataProperty.Equals(counters) || metadataProperty.Equals(attachments) || metadataProperty.Equals(timeSeries);
                    break;

                default:
                    throw new NotSupportedException($"Not supported value: {flags}");
            }
        }
        public IDatabaseRecordActions DatabaseRecord()
        {
            throw new NotImplementedException();
        }

        public IDocumentActions Documents(bool throwOnCollectionMismatchError = true)
        {
            return new StreamDocumentActions(_writer, _context, _tContext, _shardAllocations, _source, _options, _filterMetadataProperty, "Docs");
        }

        public IDocumentActions RevisionDocuments()
        {
            throw new NotImplementedException();
        }

        public IDocumentActions Tombstones()
        {
            throw new NotImplementedException();
        }

        public IDocumentActions Conflicts()
        {
            throw new NotImplementedException();
        }

        public IIndexActions Indexes()
        {
            throw new NotImplementedException();
        }

        public IKeyValueActions<long> Identities()
        {
            throw new NotImplementedException();
        }

        public ICompareExchangeActions CompareExchange(JsonOperationContext context)
        {
            throw new NotImplementedException();
        }

        public ICompareExchangeActions CompareExchangeTombstones(JsonOperationContext context)
        {
            throw new NotImplementedException();
        }

        public ICounterActions Counters(SmugglerResult result, List<IDisposable> toDispose)
        {
            throw new NotImplementedException();
        }

        public ICounterActions Counters(SmugglerResult result)
        {
            return new StreamCounterActions(_writer, _context, _tContext, _shardAllocations, this, nameof(DatabaseItemType.CounterGroups));
        }

        public ISubscriptionActions Subscriptions()
        {
            throw new NotImplementedException();
        }

        public IReplicationHubCertificateActions ReplicationHubCertificates()
        {
            throw new NotImplementedException();
        }

        public ITimeSeriesActions TimeSeries()
        {
            return new StreamTimeSeriesActions(_writer, _tContext, _shardAllocations, nameof(DatabaseItemType.TimeSeries));
        }

        private class StreamDocumentActions : StreamActionsBase, IDocumentActions
        {
            private readonly JsonOperationContext _context;
            private readonly TransactionOperationContext _tContext;
            private readonly ISmugglerSource _source;
            private readonly DatabaseSmugglerOptionsServerSide _options;
            private readonly Func<LazyStringValue, bool> _filterMetadataProperty;
            private HashSet<string> _attachmentStreamsAlreadyExported;
            private readonly List<DatabaseRecord.ShardRangeAssignment> _shardAllocation;
            public StreamDocumentActions(List<AsyncBlittableJsonTextWriter> writer, JsonOperationContext context, TransactionOperationContext tContext, 
                List<DatabaseRecord.ShardRangeAssignment>  shardAllocation, ISmugglerSource source, DatabaseSmugglerOptionsServerSide options, 
                Func<LazyStringValue, bool> filterMetadataProperty, string propertyName)
                : base( writer, propertyName)
            {
                _context = context;
                _source = source;
                _options = options;
                _tContext = tContext;
                _filterMetadataProperty = filterMetadataProperty;
                _shardAllocation = shardAllocation;
            }

            public async ValueTask WriteDocumentAsync(DocumentItem item, SmugglerProgressBase.CountsWithLastEtagAndAttachments progress)
            {
                // if (item.Attachments != null)
                //     throw new NotSupportedException();

                var document = item.Document;
                using (document)
                {

                    var index = ShardHelper.GetShardIndexforDocument(_tContext, _shardAllocation, document.Id);

                    if (item.Attachments != null)
                    {
                        foreach (var attachment in item.Attachments)
                        {
                            attachment.Stream.Position = 0;
                            await WriteAttachmentStreamAsync(attachment.Base64Hash.Content.ToString(), attachment.Stream, attachment.Tag.ToString(), index);
                        }
                    }
                    else
                    {
                        await WriteUniqueAttachmentStreamsAsync(document, progress, index); // TODO - remove
                    }


                    if (First[index] == false)
                        Writer[index].WriteComma();
                    First[index] = false;

                    Writer[index].WriteDocument(_context, document, metadataOnly: false, _filterMetadataProperty);

                    await Writer[index].MaybeFlushAsync();
                }
            }

            public async ValueTask WriteTombstoneAsync(Tombstone tombstone, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                // if (First == false)
                //     Writer.WriteComma();
                // First = false;
                //
                // using (tombstone)
                // {
                //     _context.Write(Writer, new DynamicJsonValue
                //     {
                //         ["Key"] = tombstone.LowerId,
                //         [nameof(Tombstone.Type)] = tombstone.Type.ToString(),
                //         [nameof(Tombstone.Collection)] = tombstone.Collection,
                //         [nameof(Tombstone.Flags)] = tombstone.Flags.ToString(),
                //         [nameof(Tombstone.ChangeVector)] = tombstone.ChangeVector,
                //         [nameof(Tombstone.DeletedEtag)] = tombstone.DeletedEtag,
                //         [nameof(Tombstone.Etag)] = tombstone.Etag,
                //         [nameof(Tombstone.LastModified)] = tombstone.LastModified,
                //     });
                //
                //     await Writer.MaybeFlushAsync();
                // }
            }

            public async ValueTask WriteConflictAsync(DocumentConflict conflict, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                // if (First == false)
                //     Writer.WriteComma();
                // First = false;
                //
                // using (conflict)
                // {
                //     _context.Write(Writer, new DynamicJsonValue
                //     {
                //         [nameof(DocumentConflict.Id)] = conflict.Id,
                //         [nameof(DocumentConflict.Collection)] = conflict.Collection,
                //         [nameof(DocumentConflict.Flags)] = conflict.Flags.ToString(),
                //         [nameof(DocumentConflict.ChangeVector)] = conflict.ChangeVector,
                //         [nameof(DocumentConflict.Etag)] = conflict.Etag,
                //         [nameof(DocumentConflict.LastModified)] = conflict.LastModified,
                //         [nameof(DocumentConflict.Doc)] = conflict.Doc,
                //     });
                //
                //     await Writer.MaybeFlushAsync();
                // }
            }

            public ValueTask DeleteDocumentAsync(string id)
            {
                // no-op
                return default;
            }

            public IEnumerable<DocumentItem> GetDocumentsWithDuplicateCollection()
            {
                yield break;
            }

            public Stream GetTempStream()
            {
                var tempFileName = $"{Guid.NewGuid()}.smuggler";
                return new StreamsTempFile(tempFileName, _options.EncryptionKey != null).StartNewStream();
            }

            private async ValueTask WriteUniqueAttachmentStreamsAsync(Document document, SmugglerProgressBase.CountsWithLastEtagAndAttachments progress, int index)
            {
                if ((document.Flags & DocumentFlags.HasAttachments) != DocumentFlags.HasAttachments ||
                    document.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                    metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                    return;

                if (_attachmentStreamsAlreadyExported == null)
                    _attachmentStreamsAlreadyExported = new HashSet<string>();

                foreach (BlittableJsonReaderObject attachment in attachments)
                {
                    if (attachment.TryGet(nameof(AttachmentName.Hash), out LazyStringValue hash) == false)
                    {
                        progress.Attachments.ErroredCount++;

                        throw new ArgumentException($"Hash field is mandatory in attachment's metadata: {attachment}");
                    }

                    progress.Attachments.ReadCount++;

                    if (_attachmentStreamsAlreadyExported.Add(hash))
                    {
                        await using (var stream = _source.GetAttachmentStream(hash, out string tag))
                        {
                            if (stream == null)
                            {
                                progress.Attachments.ErroredCount++;
                                throw new ArgumentException($"Document {document.Id} seems to have a attachment hash: {hash}, but no correlating hash was found in the storage.");
                            }
                            await WriteAttachmentStreamAsync(hash, stream, tag, index);
                        }
                    }
                }
            }

            public JsonOperationContext GetContextForNewDocument()
            {
                _context.CachedProperties.NewDocument();
                return _context;
            }

            private async ValueTask WriteAttachmentStreamAsync(string hash, Stream stream, string tag, int index)
            {
                if (First[index] == false)
                    Writer[index].WriteComma();
                First[index] = false;
                
                Writer[index].WriteStartObject();
                
                Writer[index].WritePropertyName(Constants.Documents.Metadata.Key);
                Writer[index].WriteStartObject();
                
                Writer[index].WritePropertyName(DocumentItem.ExportDocumentType.Key);
                Writer[index].WriteString(DocumentItem.ExportDocumentType.Attachment);
                
                Writer[index].WriteEndObject();
                Writer[index].WriteComma();
                
                Writer[index].WritePropertyName(nameof(AttachmentName.Hash));
                Writer[index].WriteString(hash);
                Writer[index].WriteComma();
                
                Writer[index].WritePropertyName(nameof(AttachmentName.Size));
                Writer[index].WriteInteger(stream.Length);
                Writer[index].WriteComma();
                
                Writer[index].WritePropertyName(nameof(DocumentItem.AttachmentStream.Tag));
                Writer[index].WriteString(tag);
                
                Writer[index].WriteEndObject();
                
                Writer[index].WriteStream(stream);
                
                await Writer[index].MaybeFlushAsync();
            }
        }

        private class StreamTimeSeriesActions : StreamActionsBase, ITimeSeriesActions
        {
            private readonly TransactionOperationContext _tContext;
            private readonly List<DatabaseRecord.ShardRangeAssignment> _shardAllocation;
            public StreamTimeSeriesActions(List<AsyncBlittableJsonTextWriter> writer, TransactionOperationContext tContext, List<DatabaseRecord.ShardRangeAssignment> shardAllocation, string propertyName) : base(writer, propertyName)
            {
                _tContext = tContext;
                _shardAllocation = shardAllocation;
            }

            public async ValueTask WriteTimeSeriesAsync(TimeSeriesItem item)
            {
                var index = ShardHelper.GetShardIndexforDocument(_tContext, _shardAllocation, item.DocId);

                if (First[index] == false)
                    Writer[index].WriteComma();
                First[index] = false;

                Writer[index].WriteStartObject();
                {
                    Writer[index].WritePropertyName(Constants.Documents.Blob.Document);

                    Writer[index].WriteStartObject();
                    {
                        Writer[index].WritePropertyName(nameof(TimeSeriesItem.DocId));
                        Writer[index].WriteString(item.DocId);
                        Writer[index].WriteComma();

                        Writer[index].WritePropertyName(nameof(TimeSeriesItem.Name));
                        Writer[index].WriteString(item.Name);
                        Writer[index].WriteComma();

                        Writer[index].WritePropertyName(nameof(TimeSeriesItem.ChangeVector));
                        Writer[index].WriteString(item.ChangeVector);
                        Writer[index].WriteComma();

                        Writer[index].WritePropertyName(nameof(TimeSeriesItem.Collection));
                        Writer[index].WriteString(item.Collection);
                        Writer[index].WriteComma();

                        Writer[index].WritePropertyName(nameof(TimeSeriesItem.Baseline));
                        Writer[index].WriteDateTime(item.Baseline, true);
                    }
                    Writer[index].WriteEndObject();

                    Writer[index].WriteComma();
                    Writer[index].WritePropertyName(Constants.Documents.Blob.Size);
                    Writer[index].WriteInteger(item.SegmentSize);
                }
                Writer[index].WriteEndObject();

                unsafe
                {
                    Writer[index].WriteMemoryChunk(item.Segment.Ptr, item.Segment.NumberOfBytes);
                }

                await Writer[index].MaybeFlushAsync();
            }
        }

        internal abstract class StreamActionsBase : IAsyncDisposable
        {
            protected readonly List<AsyncBlittableJsonTextWriter> Writer;

            protected List<bool> First { get; set; }

            protected StreamActionsBase(List<AsyncBlittableJsonTextWriter> writer , string propertyName)
            {
                Writer = writer;
                First = new List<bool>();
                for (int i = 0; i < writer.Count; i++)
                {
                    First.Add(true);
                    Writer[i].WriteComma();
                    Writer[i].WritePropertyName(propertyName);
                    Writer[i].WriteStartArray();
                }
            }

            public ValueTask DisposeAsync()
            {
                foreach (var writer in Writer)
                {
                    writer.WriteEndArray();
                }
                
                return default;
            }
        }

        private class StreamCounterActions : StreamActionsBase, ICounterActions
        {
            private readonly JsonOperationContext _context;
            private readonly ShardedDatabaseDestination _destination;
            private readonly TransactionOperationContext _tContext;
            private readonly List<DatabaseRecord.ShardRangeAssignment> _shardAllocation;

            public StreamCounterActions(List<AsyncBlittableJsonTextWriter> writer, JsonOperationContext context, TransactionOperationContext tContext,
                List<DatabaseRecord.ShardRangeAssignment> shardAllocation, ShardedDatabaseDestination destination, string propertyName) : base(writer, propertyName)
            {
                _context = context;
                _destination = destination;
                _tContext = tContext;
                _shardAllocation = shardAllocation;
            }

            public async ValueTask WriteCounterAsync(CounterGroupDetail counterDetail)
            {
                CountersStorage.ConvertFromBlobToNumbers(_tContext, counterDetail);

                var index = ShardHelper.GetShardIndexforDocument(_tContext, _shardAllocation, counterDetail.DocumentId);


                using (counterDetail)
                {
                    if (First[index] == false)
                        Writer[index].WriteComma();
                    First[index] = false;

                    Writer[index].WriteStartObject();

                    Writer[index].WritePropertyName(nameof(CounterItem.DocId));
                    Writer[index].WriteString(counterDetail.DocumentId, skipEscaping: true);
                    Writer[index].WriteComma();

                    Writer[index].WritePropertyName(nameof(CounterItem.ChangeVector));
                    Writer[index].WriteString(counterDetail.ChangeVector, skipEscaping: true);
                    Writer[index].WriteComma();

                    Writer[index].WritePropertyName(nameof(CounterItem.Batch.Values));
                    Writer[index].WriteObject(counterDetail.Values);

                    Writer[index].WriteEndObject();

                    await Writer[index].MaybeFlushAsync();
                }
            }

            public ValueTask WriteLegacyCounterAsync(CounterDetail counterDetail)
            {
                // Used only in Database Destination
                throw new NotSupportedException("WriteLegacyCounter is not supported when writing to a Stream destination, " +
                                                "it is only supported when writing to Database destination. Shouldn't happen.");
            }

            public void RegisterForDisposal(IDisposable data)
            {
                _destination.ToDispose ??= new List<IDisposable>();
                _destination.ToDispose.Add(data);
            }

            public JsonOperationContext GetContextForNewDocument()
            {
                _context.CachedProperties.NewDocument();
                return _context;
            }

            public Stream GetTempStream()
            {
                throw new NotSupportedException("GetTempStream is never used in StreamCounterActions. Shouldn't happen");
            }
        }

    }
}
