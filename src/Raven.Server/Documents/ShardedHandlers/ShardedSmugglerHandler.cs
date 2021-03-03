using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Newtonsoft.Json.Serialization;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.Documents.Sharding;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Handlers;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Threading;
using DatabaseSmuggler = Raven.Client.Documents.Smuggler.DatabaseSmuggler;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedSmugglerHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/smuggler/validate-options", "POST")]
        public async Task PostValidateOptions()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var blittableJson = await context.ReadForMemoryAsync(RequestBodyStream(), "validate-options");
                var options = JsonDeserializationServer.DatabaseSmugglerOptions(blittableJson);

                if (!string.IsNullOrEmpty(options.FileName) && options.FileName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
                    throw new InvalidOperationException($"{options.FileName} is invalid file name");

                if (string.IsNullOrEmpty(options.TransformScript))
                {
                    NoContentStatus();
                    return;
                }

                // TODO: maybe all of this isn't necessary and we can test it on any server?
                var tasks = new List<Task>();
                using (var cts = new CancellationTokenSource())
                {
                    foreach (var requestExecutor in ShardedContext.RequestExecutors)
                    {
                        tasks.Add(requestExecutor.ExecuteAsync(new ValidateOptionsCommand(options), token: cts.Token));
                    }

                    while (tasks.Count > 0)
                    {
                        var t = await Task.WhenAny(tasks);
                        if (t.IsCompletedSuccessfully == false)
                        {
                            cts.Cancel(throwOnFirstException: false);
                            await t; // throw
                            Debug.Assert(false, "Should never happened");
                            return; // should never hit
                        }

                        tasks.Remove(t);
                    }
                }

                NoContentStatus();
            }
        }

        [RavenShardedAction("/databases/*/smuggler/export", "POST")]
        public async Task PostExport()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                // TODO : this id should be shared between all dbs, so need to generate it in cluster?
                // var operationId = GetLongQueryString("operationId", false) ?? Database.Operations.GetNextOperationId();

                var operationId = GetLongQueryString("operationId", true) ?? 0;
                var startDocumentEtag = GetLongQueryString("startEtag", false) ?? 0;
                var startRaftIndex = GetLongQueryString("startRaftIndex", false) ?? 0;

                // for sharded database the idea is to have op-id as negative number, so it would not collide with any specific shard op-id
                if (operationId >= 0)
                    throw new InvalidOperationException("Operation id of sharded database has to be negative");

                // TODO: for incremental backups doc Etag and Raft Index might differ from shard to shard.
                if (startDocumentEtag > 0 || startRaftIndex > 0)
                    throw new NotImplementedException("Incremental backup is not implemented yet.");

                var stream = TryGetRequestFromStream("DownloadOptions") ?? RequestBodyStream();

                DatabaseSmugglerOptionsServerSide shardOptions = null;
                DatabaseSmugglerOptionsServerSide globalOptions = null;
                using (context.GetMemoryBuffer(out var buffer))
                {
                    var firstRead = await stream.ReadAsync(buffer.Memory);
                    buffer.Used = 0;
                    buffer.Valid = firstRead;
                    if (firstRead != 0)
                    {
                        var blittableJson = await context.ParseToMemoryAsync(stream, "DownloadOptions", BlittableJsonDocumentBuilder.UsageMode.None, buffer);
                        shardOptions = JsonDeserializationServer.DatabaseSmugglerOptions(blittableJson);
                        globalOptions = JsonDeserializationServer.DatabaseSmugglerOptions(blittableJson);
                    }
                }

                globalOptions ??= new DatabaseSmugglerOptionsServerSide();
                globalOptions.OperateOnTypes &= DatabaseItemType.ClusterTypes;

                if (shardOptions != null)
                {
                    // we are not rely on the shrads to get the database record stuff
                    shardOptions.OperateOnDatabaseRecordTypes = DatabaseRecordItemType.None;
                    shardOptions.OperateOnTypes &= ~DatabaseItemType.ClusterTypes;
                }

                if (string.IsNullOrWhiteSpace(shardOptions?.EncryptionKey) == false)
                    ServerStore.LicenseManager.AssertCanCreateEncryptedDatabase();


                var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;

                if (feature == null)
                    globalOptions.AuthorizationStatus = AuthorizationStatus.DatabaseAdmin;
                else
                    globalOptions.AuthorizationStatus = feature.CanAccess(ShardedContext.DatabaseName, requireAdmin: true)
                        ? AuthorizationStatus.DatabaseAdmin
                        : AuthorizationStatus.ValidUser;

                var token = new OperationCancelToken(ServerStore.ServerShutdown);

                await using (var outputStream = SmugglerHandler.GetOutputStream(ResponseBodyStream(), globalOptions))
                await using (var gzipStream = new GZipStream(outputStream, CompressionMode.Compress, leaveOpen: true))
                using (var writer = new BlittableJsonTextWriter(context, gzipStream))
                {
                    var shardExportTasks = new List<Task>();
                    foreach (var requestExecutor in ShardedContext.RequestExecutors)
                    {
                        shardExportTasks.Add(requestExecutor.ExecuteAsync(new ShardedExportCommand(writer, shardOptions, operationId), token: token.Token));
                    }

                    var fileName = globalOptions.FileName;
                    if (string.IsNullOrEmpty(fileName))
                    {
                        fileName = $"Dump of {ShardedContext.DatabaseName} {SystemTime.UtcNow.ToString("yyyy-MM-dd HH-mm", CultureInfo.InvariantCulture)}";
                    }

                    var contentDisposition = "attachment; filename=" + Uri.EscapeDataString(fileName) + ".ravendbdump";
                    HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
                    HttpContext.Response.Headers["Content-Type"] = "application/octet-stream";
                }
                

                /*try
                {
                    await MetricCacher.Keys.Database.Operations.AddOperation(
                            MetricCacher.Keys.Database,
                            "Export database: " + MetricCacher.Keys.Database.Name,
                            Operations.OperationType.DatabaseExport,
                            onProgress => Task.Run(() => ExportDatabaseInternal(shardOptions, startDocumentEtag, startRaftIndex, onProgress, context, token), token.Token), operationId, token: token);
                }
                catch (Exception)
                {
                    HttpContext.Abort();
                }*/
            }
        }

        private class ValidateOptionsCommand : RavenCommand
        {
            private readonly DatabaseSmugglerOptionsServerSide _options;
            public override bool IsReadRequest => false;

            public ValidateOptionsCommand(DatabaseSmugglerOptionsServerSide options)
            {
                _options = options;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/smuggler/validate-options";
                var config = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_options, ctx);

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        ctx.Write(stream, config);
                    })
                };
            }
        }

        private class ShardedExportCommand : RavenCommand
        {
            private readonly BlittableJsonTextWriter _globalWriter;
            private readonly DatabaseSmugglerOptionsServerSide _options;
            private readonly long _operationId;
            public override bool IsReadRequest => false;

            public ShardedExportCommand(BlittableJsonTextWriter globalWriter, DatabaseSmugglerOptionsServerSide options, long operationId)
            {
                _globalWriter = globalWriter;
                _options = options;
                _operationId = operationId;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/smuggler/export?operationId={_operationId}";
                var request = new HttpRequestMessage {Method = HttpMethod.Post};

                if (_options != null)
                {
                    var options = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_options, ctx);
                    request.Content = new BlittableJsonContent(stream =>
                    {
                        ctx.Write(stream, options);
                    });
                }

                return request;
            }

            public override async Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
            {
                await using (var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
                await using (var source = new GZipStream(new BufferedStream(stream, 128 * Voron.Global.Constants.Size.Kilobyte), CompressionMode.Decompress))
                {
                    var from = new StreamSource("shard-x", source, context, new SystemTime(), SharedMultipleUseFlag.None);
                    var to = new SharedStreamDestination(_globalWriter, context, from);

                    var smuggler = new Smuggler.Documents.DatabaseSmuggler(null, from, to, new SystemTime(), _options);

                    smuggler.Execute();
                }

                return ResponseDisposeHandling.Automatic;
            }
        }

        public class SharedState
        {
            private readonly int _count;
            private readonly ConcurrentDictionary<string, State> _states = new ConcurrentDictionary<string, State>();
            private readonly ConcurrentSet<string> _attachmentStreamsAlreadyExported = new ConcurrentSet<string>();


            public SharedState(int count)
            {
                _count = count;
            }

            public bool IsFirst(string type)
            {
                var state = _states.GetOrAdd(type, new State
                {
                    Processed = _count
                });
                return Interlocked.CompareExchange(ref state.First, 1, 0) == 0;
            }

            public bool InitType(string type, Action init)
            {
                var state = _states.GetOrAdd(type, new State());
                Interlocked.Decrement(ref state.Processed);
                return Interlocked.CompareExchange(ref state.Init, 1, 0) == 0;
            }

            public void FinishType(string type, Action finish)
            {
                var state = _states[type];
                if (state == null)
                    throw new InvalidOperationException($"The type {type} isn't initialized.");

                // reach to zero when all init
                // reach to -count when all done
                if (Interlocked.Decrement(ref state.Processed) > -_count)
                    return;

                finish();
            }

            public bool SeenAttachment(string attachment)
            {
                return _attachmentStreamsAlreadyExported.TryAdd(attachment);
            }

            private class State
            {
                public int First;
                public int Init;
                public int Processed;
            }
        }

        public class SharedStreamDestination : ISmugglerDestination
        {
            private readonly JsonOperationContext _context;
            private readonly ISmugglerSource _source;
            private readonly SharedState _state;
            private readonly BlittableJsonTextWriter _writer;
            private DatabaseSmugglerOptionsServerSide _options;
            private Func<LazyStringValue, bool> _filterMetadataProperty;

            public SharedStreamDestination(BlittableJsonTextWriter writer, JsonOperationContext context, ISmugglerSource source, SharedState state)
            {
                _writer = writer;
                _context = context;
                _source = source;
                _state = state;
            }

            public IDisposable Initialize(DatabaseSmugglerOptionsServerSide options, SmugglerResult result, long buildVersion)
            {
                _options = options;

                SetupMetadataFilterMethod(_context);

                if (_state.IsFirst("init"))
                {
                    _writer.WriteStartObject();

                    _writer.WritePropertyName("BuildVersion");
                    _writer.WriteInteger(buildVersion);
                }

                return new DisposableAction(() =>
                {
                    if (_state.IsFirst("dispose"))
                    {
                        _writer.WriteEndObject();
                        _writer.Flush();
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
                        _filterMetadataProperty = metadataProperty =>
                            metadataProperty.Equals(counters) || metadataProperty.Equals(attachments) || metadataProperty.Equals(timeSeries);
                        break;
                    default:
                        throw new NotSupportedException($"Not supported value: {flags}");
                }
            }

            public IDocumentActions Documents()
            {
                return new StreamDocumentActions(_writer, _context, _source, _options, _filterMetadataProperty, "Docs", _state);
            }

            public IDocumentActions RevisionDocuments()
            {
                return new StreamDocumentActions(_writer, _context, _source, _options, _filterMetadataProperty, nameof(DatabaseItemType.RevisionDocuments), _state);
            }

            public IDocumentActions Tombstones()
            {
                return new StreamDocumentActions(_writer, _context, _source, _options, _filterMetadataProperty, nameof(DatabaseItemType.Tombstones), _state);
            }

            public IDocumentActions Conflicts()
            {
                return new StreamDocumentActions(_writer, _context, _source, _options, _filterMetadataProperty, nameof(DatabaseItemType.Conflicts), _state);
            }

            public ITimeSeriesActions TimeSeries()
            {
                return new StreamTimeSeriesActions(_writer, _context, nameof(DatabaseItemType.TimeSeries), _state);
            }

            public ICounterActions Counters(SmugglerResult result)
            {
                return new StreamCounterActions(_writer, _context, nameof(DatabaseItemType.CounterGroups), _state);
            }

            private class StreamDocumentActions : ConcurrentStreamActionsBase, IDocumentActions
            {
                private readonly JsonOperationContext _context;
                private readonly ISmugglerSource _source;
                private readonly DatabaseSmugglerOptionsServerSide _options;
                private readonly Func<LazyStringValue, bool> _filterMetadataProperty;

                public StreamDocumentActions(BlittableJsonTextWriter writer, JsonOperationContext context, ISmugglerSource source,
                    DatabaseSmugglerOptionsServerSide options, Func<LazyStringValue, bool> filterMetadataProperty, string propertyName, SharedState state)
                    : base(writer, propertyName, state)
                {
                    _context = context;
                    _source = source;
                    _options = options;
                    _filterMetadataProperty = filterMetadataProperty;
                }

                public void WriteDocument(DocumentItem item, SmugglerProgressBase.CountsWithLastEtagAndAttachments progress)
                {
                    if (item.Attachments != null)
                        throw new NotSupportedException();

                    var document = item.Document;
                    using (document)
                    {
                        if (_options.OperateOnTypes.HasFlag(DatabaseItemType.Attachments))
                            WriteUniqueAttachmentStreams(document, progress);

                        lock (Writer)
                        {
                            if (IsFirst == false)
                                Writer.WriteComma();

                            Writer.WriteDocument(_context, document, metadataOnly: false, _filterMetadataProperty);
                        }
                    }
                }

                public void WriteTombstone(Tombstone tombstone, SmugglerProgressBase.CountsWithLastEtag progress)
                {
                    lock (Writer)
                    {
                        if (IsFirst == false)
                            Writer.WriteComma();

                        using (tombstone)
                        {
                            _context.Write(Writer, new DynamicJsonValue
                            {
                                ["Key"] = tombstone.LowerId,
                                [nameof(Tombstone.Type)] = tombstone.Type.ToString(),
                                [nameof(Tombstone.Collection)] = tombstone.Collection,
                                [nameof(Tombstone.Flags)] = tombstone.Flags.ToString(),
                                [nameof(Tombstone.ChangeVector)] = tombstone.ChangeVector,
                                [nameof(Tombstone.DeletedEtag)] = tombstone.DeletedEtag,
                                [nameof(Tombstone.Etag)] = tombstone.Etag,
                                [nameof(Tombstone.LastModified)] = tombstone.LastModified,
                            });
                        }
                    }
                }

                public void WriteConflict(DocumentConflict conflict, SmugglerProgressBase.CountsWithLastEtag progress)
                {
                    lock (Writer)
                    {
                        if (IsFirst == false)
                            Writer.WriteComma();

                        using (conflict)
                        {
                            _context.Write(Writer, new DynamicJsonValue
                            {
                                [nameof(DocumentConflict.Id)] = conflict.Id,
                                [nameof(DocumentConflict.Collection)] = conflict.Collection,
                                [nameof(DocumentConflict.Flags)] = conflict.Flags.ToString(),
                                [nameof(DocumentConflict.ChangeVector)] = conflict.ChangeVector,
                                [nameof(DocumentConflict.Etag)] = conflict.Etag,
                                [nameof(DocumentConflict.LastModified)] = conflict.LastModified,
                                [nameof(DocumentConflict.Doc)] = conflict.Doc,
                            });
                        }
                    }
                }

                public void DeleteDocument(string id)
                {
                    // no-op
                }

                public Stream GetTempStream()
                {
                    throw new NotSupportedException();
                }

                private void WriteUniqueAttachmentStreams(Document document, SmugglerProgressBase.CountsWithLastEtagAndAttachments progress)
                {
                    if ((document.Flags & DocumentFlags.HasAttachments) != DocumentFlags.HasAttachments ||
                        document.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                        metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                        return;

                    foreach (BlittableJsonReaderObject attachment in attachments)
                    {
                        if (attachment.TryGet(nameof(AttachmentName.Hash), out LazyStringValue hash) == false)
                        {
                            progress.Attachments.ErroredCount++;

                            throw new ArgumentException($"Hash field is mandatory in attachment's metadata: {attachment}");
                        }

                        progress.Attachments.ReadCount++;

                        if (SeenAttachment(hash))
                        {
                            using (var stream = _source.GetAttachmentStream(hash, out string tag))
                            {
                                if (stream == null)
                                {
                                    progress.Attachments.ErroredCount++;
                                    throw new ArgumentException(
                                        $"Document {document.Id} seems to have a attachment hash: {hash}, but no correlating hash was found in the storage.");
                                }

                                lock (Writer)
                                {
                                    WriteAttachmentStream(hash, stream, tag);
                                }
                            }
                        }
                    }
                }

                public JsonOperationContext GetContextForNewDocument()
                {
                    _context.CachedProperties.NewDocument();
                    return _context;
                }

                private void WriteAttachmentStream(LazyStringValue hash, Stream stream, string tag)
                {
                    if (IsFirst == false)
                        Writer.WriteComma();

                    Writer.WriteStartObject();

                    Writer.WritePropertyName(Constants.Documents.Metadata.Key);
                    Writer.WriteStartObject();

                    Writer.WritePropertyName(DocumentItem.ExportDocumentType.Key);
                    Writer.WriteString(DocumentItem.ExportDocumentType.Attachment);

                    Writer.WriteEndObject();
                    Writer.WriteComma();

                    Writer.WritePropertyName(nameof(AttachmentName.Hash));
                    Writer.WriteString(hash);
                    Writer.WriteComma();

                    Writer.WritePropertyName(nameof(AttachmentName.Size));
                    Writer.WriteInteger(stream.Length);
                    Writer.WriteComma();

                    Writer.WritePropertyName(nameof(DocumentItem.AttachmentStream.Tag));
                    Writer.WriteString(tag);

                    Writer.WriteEndObject();

                    Writer.WriteStream(stream);
                }

            }

            private class StreamCounterActions : ConcurrentStreamActionsBase, ICounterActions
            {
                private readonly JsonOperationContext _context;

                public StreamCounterActions(BlittableJsonTextWriter writer, JsonOperationContext context, string propertyName, SharedState state) : base(writer, propertyName, state)
                {
                    _context = context;
                }

                public void WriteCounter(CounterGroupDetail counterDetail)
                {
                    CountersStorage.ConvertFromBlobToNumbers(_context, counterDetail);

                    using (counterDetail)
                    {
                        lock (Writer)
                        {
                            if (IsFirst == false)
                                Writer.WriteComma();

                            Writer.WriteStartObject();

                            Writer.WritePropertyName(nameof(CounterItem.DocId));
                            Writer.WriteString(counterDetail.DocumentId, skipEscaping: true);
                            Writer.WriteComma();

                            Writer.WritePropertyName(nameof(CounterItem.ChangeVector));
                            Writer.WriteString(counterDetail.ChangeVector, skipEscaping: true);
                            Writer.WriteComma();

                            Writer.WritePropertyName(nameof(CounterItem.Batch.Values));
                            Writer.WriteObject(counterDetail.Values);

                            Writer.WriteEndObject();
                        }
                    }
                }

                public void WriteLegacyCounter(CounterDetail counterDetail)
                {
                    // Used only in Database Destination 
                    throw new NotSupportedException("WriteLegacyCounter is not supported when writing to a Stream destination, " +
                                                    "it is only supported when writing to Database destination. Shouldn't happen.");
                }

                public void RegisterForDisposal(IDisposable data)
                {
                    throw new NotSupportedException("RegisterForDisposal is never used in StreamCounterActions. Shouldn't happen.");
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


            private class StreamTimeSeriesActions : ConcurrentStreamActionsBase, ITimeSeriesActions
            {

                public StreamTimeSeriesActions(BlittableJsonTextWriter writer, JsonOperationContext context, string propertyName, SharedState state) : base(writer, propertyName, state)
                {
                }

                public unsafe void WriteTimeSeries(TimeSeriesItem item)
                {
                    lock (Writer)
                    {
                        if (IsFirst == false)
                            Writer.WriteComma();

                        Writer.WriteStartObject();
                        {
                            Writer.WritePropertyName(Constants.Documents.Blob.Document);

                            Writer.WriteStartObject();
                            {
                                Writer.WritePropertyName(nameof(TimeSeriesItem.DocId));
                                Writer.WriteString(item.DocId);
                                Writer.WriteComma();

                                Writer.WritePropertyName(nameof(TimeSeriesItem.Name));
                                Writer.WriteString(item.Name);
                                Writer.WriteComma();

                                Writer.WritePropertyName(nameof(TimeSeriesItem.ChangeVector));
                                Writer.WriteString(item.ChangeVector);
                                Writer.WriteComma();

                                Writer.WritePropertyName(nameof(TimeSeriesItem.Collection));
                                Writer.WriteString(item.Collection);
                                Writer.WriteComma();

                                Writer.WritePropertyName(nameof(TimeSeriesItem.Baseline));
                                Writer.WriteDateTime(item.Baseline, true);
                            }
                            Writer.WriteEndObject();

                            Writer.WriteComma();
                            Writer.WritePropertyName(Constants.Documents.Blob.Size);
                            Writer.WriteInteger(item.SegmentSize);
                        }
                        Writer.WriteEndObject();

                        Writer.WriteMemoryChunk(item.Segment.Ptr, item.Segment.NumberOfBytes);
                    }
                }
            }

            // TODO: make it lock-free if needed
            private abstract class ConcurrentStreamActionsBase : IDisposable
            {
                private readonly string _propertyName;
                private readonly SharedState _state;
                protected readonly BlittableJsonTextWriter Writer;

                protected ConcurrentStreamActionsBase(BlittableJsonTextWriter writer, string propertyName, SharedState state)
                {
                    _propertyName = propertyName;
                    _state = state;

                    lock (writer)
                    {
                        if (_state.TryInitType(propertyName) == false)
                            return;

                        Writer = writer;

                        Writer.WriteComma();
                        Writer.WritePropertyName(propertyName);
                        Writer.WriteStartArray();
                    }
                }

                protected bool IsFirst => _state.IsFirst(_propertyName);

                protected bool SeenAttachment(string name) => _state.SeenAttachment(name);

                public void Dispose()
                {
                    _state.FinishType(_propertyName, () =>
                    {
                        lock (Writer)
                        {
                            Writer.WriteEndArray();
                        }
                    });
                }
            }

            // all cluster entities are exported via the usual StreamDestination

            public IDatabaseRecordActions DatabaseRecord()
            {
                throw new NotSupportedException("DatabaseRecord isn't supported in a sharded stream destination");
            }

            public IIndexActions Indexes()
            {
                throw new NotSupportedException("Indexes aren't supported in a sharded stream destination");
            }

            public IKeyValueActions<long> Identities()
            {
                throw new NotSupportedException("Identities aren't supported in a sharded stream destination");
            }

            public ICompareExchangeActions CompareExchange(JsonOperationContext context)
            {
                throw new NotSupportedException("CompareExchange isn't supported in a sharded stream destination");
            }

            public ICompareExchangeActions CompareExchangeTombstones(JsonOperationContext context)
            {
                throw new NotSupportedException("CompareExchangeTombstones aren't supported in a sharded stream destination");
            }

            public ISubscriptionActions Subscriptions()
            {
                throw new NotSupportedException("Subscriptions aren't supported in a sharded stream destination");
            }

            public IReplicationHubCertificateActions ReplicationHubCertificates()
            {
                throw new NotSupportedException("ReplicationHubCertificates aren't supported in a sharded stream destination");
            }
        }
    }
}
