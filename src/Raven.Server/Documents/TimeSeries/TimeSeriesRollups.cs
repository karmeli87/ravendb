﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.TimeSeries
{
    public class TimeSeriesRollups
    {
        private static readonly TableSchema RollupSchema;
        public static readonly Slice TimeSeriesRollupTable;
        private static readonly Slice RollupKey;
        private static readonly Slice NextRollupIndex;
        private enum RollupColumns
        {
            // documentId/Name
            Key,
            Collection,
            NextRollup,
            PolicyToApply,
            Etag,
            ChangeVector
        }

        internal class RollupState
        {
            public Slice Key;
            public string DocId;
            public string Name;
            public long Etag;
            public string Collection;
            public DateTime NextRollup;
            public string RollupPolicy;
            public string ChangeVector;

            public override string ToString()
            {
                return $"Rollup for time-series '{Name}' in document '{DocId}' of policy {RollupPolicy} at {NextRollup}";
            }
        }

        static TimeSeriesRollups()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, nameof(TimeSeriesRollupTable), ByteStringType.Immutable, out TimeSeriesRollupTable);
                Slice.From(ctx, nameof(RollupKey), ByteStringType.Immutable, out RollupKey);
                Slice.From(ctx, nameof(NextRollupIndex), ByteStringType.Immutable, out NextRollupIndex);
            }

            RollupSchema = new TableSchema();
            RollupSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)RollupColumns.Key,
                Count = 1, 
                Name = RollupKey
            });

            RollupSchema.DefineIndex(new TableSchema.SchemaIndexDef // this isn't fixed-size since we expect to have duplicates
            {
                StartIndex = (int)RollupColumns.NextRollup, 
                Count = 1,
                Name = NextRollupIndex
            });
        }

        private readonly Logger _logger;

        public TimeSeriesRollups(string database)
        {
            _logger = LoggingSource.Instance.GetLogger<TimeSeriesPolicyRunner>(database);
        }

        public unsafe void MarkForPolicy(DocumentsOperationContext context, TimeSeriesSliceHolder slicerHolder, TimeSeriesPolicy nextPolicy, DateTime timestamp)
        {
            var nextRollup = NextRollup(timestamp, nextPolicy);

            // mark for rollup
            RollupSchema.Create(context.Transaction.InnerTransaction, TimeSeriesRollupTable, 16);
            var table = context.Transaction.InnerTransaction.OpenTable(RollupSchema, TimeSeriesRollupTable);
            using (table.Allocate(out var tvb))
            using (Slice.From(context.Allocator, nextPolicy.Name, ByteStringType.Immutable, out var policyToApply))
            {
                if (table.ReadByKey(slicerHolder.StatsKey, out var tvr))
                {
                    // check if we need to update this
                    var existingRollup = Bits.SwapBytes(*(long*)tvr.Read((int)RollupColumns.NextRollup, out _));
                    if (existingRollup <= nextRollup)
                        return; // we have an earlier date to roll up from
                }

                if (_logger.IsInfoEnabled)
                    _logger.Info(
                        $"Marking {slicerHolder.Name} in document {slicerHolder.DocId} for policy {nextPolicy.Name} to rollup at {new DateTime(nextRollup)} (ticks:{nextRollup})");

                var etag = context.DocumentDatabase.DocumentsStorage.GenerateNextEtag();
                var changeVector = context.DocumentDatabase.DocumentsStorage.GetNewChangeVector(context, etag);
                using (Slice.From(context.Allocator, changeVector, ByteStringType.Immutable, out var changeVectorSlice))
                {
                    tvb.Add(slicerHolder.StatsKey);
                    tvb.Add(slicerHolder.CollectionSlice);
                    tvb.Add(Bits.SwapBytes(nextRollup));
                    tvb.Add(policyToApply);
                    tvb.Add(etag);
                    tvb.Add(changeVectorSlice);
                }

                table.Set(tvb);
            }
        }

        internal void PrepareRollups(DocumentsOperationContext context, DateTime currentTime, long take, List<RollupState> states, out Stopwatch duration)
        {
            duration = Stopwatch.StartNew();

            var table = context.Transaction.InnerTransaction.OpenTable(RollupSchema, TimeSeriesRollupTable);
            if (table == null)
                return;

            var currentTicks = currentTime.Ticks;

            foreach (var item in table.SeekForwardFrom(RollupSchema.Indexes[NextRollupIndex], Slices.BeforeAllKeys, 0))
            {
                if (take <= 0)
                    return;

                var rollUpTime = DocumentsStorage.TableValueToEtag((int)RollupColumns.NextRollup, ref item.Result.Reader);
                if (rollUpTime > currentTicks)
                    return;

                DocumentsStorage.TableValueToSlice(context, (int)RollupColumns.Key, ref item.Result.Reader,out var key);
                SplitKey(key, out var docId, out var name);

                var state = new RollupState
                {
                    Key = key,
                    DocId = docId,
                    Name = name,
                    Collection = DocumentsStorage.TableValueToId(context, (int)RollupColumns.Collection, ref item.Result.Reader),
                    NextRollup = new DateTime(rollUpTime),
                    RollupPolicy = DocumentsStorage.TableValueToString(context, (int)RollupColumns.PolicyToApply, ref item.Result.Reader),
                    Etag = DocumentsStorage.TableValueToLong((int)RollupColumns.Etag, ref item.Result.Reader),
                    ChangeVector = DocumentsStorage.TableValueToChangeVector(context, (int)RollupColumns.ChangeVector, ref item.Result.Reader)
                };

                if (_logger.IsInfoEnabled)
                    _logger.Info($"{state} is prepared.");

                states.Add(state);
                take--;
            }
        }

        public static void SplitKey(Slice key, out string docId, out string name)
        {
            var bytes = key.AsSpan();
            var separatorIndex = key.Content.IndexOf(SpecialChars.RecordSeparator);

            docId = Encoding.UTF8.GetString(bytes.Slice(0, separatorIndex));
            var index = separatorIndex + 1;
            name = Encoding.UTF8.GetString(bytes.Slice(index, bytes.Length - index));
        }

        internal class TimeSeriesRetentionCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public const int BatchSize = 1024;

            private readonly List<Slice> _keys;
            private readonly string _collection;
            private readonly DateTime _to;

            public TimeSeriesRetentionCommand(List<Slice> keys, string collection, DateTime to)
            {
                _keys = keys;
                _collection = collection;
                _to = to;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var logger = LoggingSource.Instance.GetLogger<TimeSeriesPolicyRunner>(context.DocumentDatabase.Name);
                var request = new TimeSeriesStorage.DeletionRangeRequest
                {
                    From = DateTime.MinValue,
                    To = _to,
                    Collection = _collection
                };

                var retained = 0;
                foreach (var key in _keys)
                {
                    SplitKey(key, out var docId, out var name);
                         
                    request.Name = name;
                    request.DocumentId = docId;

                    var done = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage.RemoveTimestampRange(context, request) != null;
                    if (done)
                        retained++;

                    if (logger.IsInfoEnabled)
                        logger.Info($"{request} was executed (successfully: {done})");
                }

                return retained;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new TimeSeriesRetentionCommandDto(_keys, _collection, _to);
            }

            public class TimeSeriesRetentionCommandDto : TransactionOperationsMerger.IReplayableCommandDto<TimeSeriesRetentionCommand>
            {
                public List<Slice> _keys;
                public string _collection;
                public DateTime _to;

                public TimeSeriesRetentionCommandDto(List<Slice> keys, string collection, DateTime to)
                {
                    _keys = keys;
                    _collection = collection;
                    _to = to;
                }
                public TimeSeriesRetentionCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
                {
                    var keys = new List<Slice>();
                    foreach (var key in _keys)
                    {
                        keys.Add(key.Clone(context.Allocator));
                    }

                    return new TimeSeriesRetentionCommand(keys, _collection, _to);
                }
            }
        }

        internal class AddedNewRollupPoliciesCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public const int BatchSize = 1024;
            private readonly CollectionName _collection;
            private readonly TimeSeriesPolicy _from;
            private readonly TimeSeriesPolicy _to;
            private readonly int _skip;

            public int Marked;

            public AddedNewRollupPoliciesCommand(CollectionName collection, TimeSeriesPolicy from, TimeSeriesPolicy to, int skip)
            {
                _collection = collection;
                _from = from;
                _to = to;
                _skip = skip;
            }
            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var tss = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage;
                RollupSchema.Create(context.Transaction.InnerTransaction, TimeSeriesRollupTable, 16);
                var table = context.Transaction.InnerTransaction.OpenTable(RollupSchema, TimeSeriesRollupTable);
                foreach (var key in tss.Stats.GetTimeSeriesNameByPolicy(context, _collection, _from.Name, _skip, BatchSize))
                {
                    using (table.Allocate(out var tvb))
                    using (DocumentIdWorker.GetStringPreserveCase(context, _collection.Name, out var collectionSlice))
                    using (Slice.From(context.Allocator, _to.Name, ByteStringType.Immutable, out var policyToApply))
                    using (Slice.From(context.Allocator, string.Empty, ByteStringType.Immutable, out var changeVectorSlice))
                    {
                        tvb.Add(key);
                        tvb.Add(collectionSlice);
                        tvb.Add(Bits.SwapBytes(_to.AggregationTime.Ticks));
                        tvb.Add(policyToApply);
                        tvb.Add(0);
                        tvb.Add(changeVectorSlice);

                        table.Set(tvb);
                    }

                    Marked++;
                }

                return Marked;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new AddedNewRollupPoliciesCommandDto(_collection, _from, _to, _skip);
            }

            public class AddedNewRollupPoliciesCommandDto : TransactionOperationsMerger.IReplayableCommandDto<AddedNewRollupPoliciesCommand>
            {
                public CollectionName _name;
                public TimeSeriesPolicy _from;
                public TimeSeriesPolicy _to;
                public int _skip;

                public AddedNewRollupPoliciesCommandDto(CollectionName name, TimeSeriesPolicy from, TimeSeriesPolicy to, int skip)
                {
                    _name = name;
                    _from = @from;
                    _to = to;
                    _skip = skip;
                }

                public AddedNewRollupPoliciesCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
                {
                    return new AddedNewRollupPoliciesCommand(_name, _from, _to, _skip);
                }
            }
        }

        internal class RollupTimeSeriesCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly TimeSeriesConfiguration _configuration;
            private readonly DateTime _now;
            private readonly List<RollupState> _states;
            private readonly bool _isFirstInTopology;

            public long RolledUp;

            internal RollupTimeSeriesCommand(TimeSeriesConfiguration configuration, DateTime now, List<RollupState> states, bool isFirstInTopology)
            {
                _configuration = configuration;
                _now = now;
                _states = states;
                _isFirstInTopology = isFirstInTopology;
            }
            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var tss = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage;
                RollupSchema.Create(context.Transaction.InnerTransaction, TimeSeriesRollupTable, 16);
                var table = context.Transaction.InnerTransaction.OpenTable(RollupSchema, TimeSeriesRollupTable);

                foreach (var item in _states)
                {
                    if (_configuration == null)
                        return RolledUp;

                    if (_configuration.Collections.TryGetValue(item.Collection, out var config) == false)
                        continue;

                    if (config.Disabled)
                        continue;
                        
                    var policy = config.GetPolicyByName(item.RollupPolicy, out _);
                    if (policy == null)
                        continue;

                    if (table.ReadByKey(item.Key, out var current) == false)
                    {
                        table.DeleteByKey(item.Key);
                        continue;
                    }

                    if (item.Etag != DocumentsStorage.TableValueToLong((int)RollupColumns.Etag, ref current))
                        continue; // concurrency check

                    var rollupStart = item.NextRollup.Add(-policy.AggregationTime);
                    var rawTimeSeries = item.Name.Split(TimeSeriesConfiguration.TimeSeriesRollupSeparator)[0];
                    var intoTimeSeries = policy.GetTimeSeriesName(rawTimeSeries);
                    
                    var intoReader = tss.GetReader(context, item.DocId, intoTimeSeries, rollupStart, DateTime.MaxValue);
                    var previouslyAggregated = intoReader.AllValues().Any();
                    if (previouslyAggregated)
                    {
                        var changeVector = intoReader.GetCurrentSegmentChangeVector();
                        if (ChangeVectorUtils.GetConflictStatus(item.ChangeVector, changeVector) == ConflictStatus.AlreadyMerged)
                        {
                            // this rollup is already done
                            table.DeleteByKey(item.Key);
                            continue;
                        }
                    }

                    if (_isFirstInTopology == false)
                        continue; // we execute the actual rollup only on the primary node to avoid conflicts

                    var rollupEnd = new DateTime(NextRollup(_now.Add(-policy.AggregationTime), policy));
                    var reader = tss.GetReader(context, item.DocId, item.Name, rollupStart, rollupEnd);

                    if (previouslyAggregated)
                    {
                        var hasPriorValues = tss.GetReader(context, item.DocId, item.Name, DateTime.MinValue, rollupStart).AllValues().Any();
                        if (hasPriorValues == false) 
                        {
                            // if the 'from' time-series doesn't have any values it is retained.
                            // so we need to aggregate only from the next time frame
                            var first = tss.GetReader(context, item.DocId, item.Name, rollupStart, DateTime.MaxValue).AllValues().FirstOrDefault();
                            if (first == default)
                                continue; // nothing we can do here

                            using (var slicer = new TimeSeriesSliceHolder(context, item.DocId, item.Name, item.Collection))
                            {
                                tss.Rollups.MarkForPolicy(context, slicer, policy, first.Timestamp);
                            }
                            continue;
                        }
                    }

                    // rollup from the the raw data will generate 6-value roll up of (first, last, min, max, sum, count)
                    // other rollups will aggregate each of those values by the type
                    var mode = item.Name.Contains(TimeSeriesConfiguration.TimeSeriesRollupSeparator) ? AggregationMode.FromAggregated : AggregationMode.FromRaw;
                    var values = GetAggregatedValues(reader, policy.AggregationTime, mode);

                    if (previouslyAggregated)
                    {
                        // if we need to re-aggregate we need to delete everything we have from that point on.  
                        var removeRequest = new TimeSeriesStorage.DeletionRangeRequest
                        {
                            Collection = item.Collection,
                            DocumentId = item.DocId,
                            Name = intoTimeSeries,
                            From = rollupStart,
                            To = DateTime.MaxValue,
                        };

                        tss.RemoveTimestampRange(context, removeRequest);
                    }
                    
                    tss.AppendTimestamp(context, item.DocId, item.Collection, intoTimeSeries, values);
                    RolledUp++;
                    table.DeleteByKey(item.Key);

                    var stats = tss.Stats.GetStats(context, item.DocId, item.Name);
                    if (stats.End > rollupEnd)
                    {
                        // we know that we have values after the current rollup and we need to mark them
                        var nextRollup = new DateTime(NextRollup(rollupEnd, policy));
                        intoReader = tss.GetReader(context, item.DocId, intoTimeSeries, nextRollup, DateTime.MaxValue);
                        if (intoReader.Init() == false)
                        {
                            Debug.Assert(false,"We have values but no segment?");
                            continue;
                        }

                        using (var slicer = new TimeSeriesSliceHolder(context, item.DocId, item.Name, item.Collection))
                        {
                            tss.Rollups.MarkForPolicy(context, slicer, policy, nextRollup);
                        }
                    }
                }

                return RolledUp;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new RollupTimeSeriesCommandDto(_configuration, _now, _states, _isFirstInTopology);
            }

            public class RollupTimeSeriesCommandDto : TransactionOperationsMerger.IReplayableCommandDto<RollupTimeSeriesCommand>
            {
                public TimeSeriesConfiguration _configuration;
                public DateTime _now;
                public List<RollupState> _states;
                public bool _isFirstInTopology;

                public RollupTimeSeriesCommandDto(TimeSeriesConfiguration configuration, DateTime now, List<RollupState> states, bool isFirstInTopology)
                {
                    _configuration = configuration;
                    _now = now;
                    _states = states;
                    _isFirstInTopology = isFirstInTopology;
                }

                public RollupTimeSeriesCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
                {
                    return new RollupTimeSeriesCommand(_configuration, _now, _states, _isFirstInTopology);
                }
            }
        }

         public enum AggregationMode
        {
            FromRaw,
            FromAggregated
        }

        private static readonly AggregationType[] Aggregations = {
            AggregationType.First,
            AggregationType.Last,
            AggregationType.Min,
            AggregationType.Max,
            AggregationType.Sum,
            AggregationType.Count
        };

        public struct TimeSeriesAggregation
        {
            private readonly AggregationMode _mode;
            public bool Any => Values.Count > 0;

            public readonly List<double> Values;

            public TimeSeriesAggregation(AggregationMode mode)
            {
                _mode = mode;
                Values = new List<double>();
            }

            public void Init()
            {
                Values.Clear();
            }

            public void Segment(Span<StatefulTimestampValue> values)
            {
                EnsureNumberOfValues(values.Length);

                for (int i = 0; i < values.Length; i++)
                {
                    var val = values[i];
                    switch (_mode)
                    {
                        case AggregationMode.FromRaw:
                            for (var index = 0; index < Aggregations.Length; index++)
                            {
                                var aggregation = Aggregations[index];
                                var aggIndex = index + (i * Aggregations.Length);
                                AggregateOnceBySegment(aggregation, aggIndex, val);
                            }

                            break;
                        case AggregationMode.FromAggregated:
                            {
                                var aggIndex = i % Aggregations.Length;
                                var aggType = Aggregations[aggIndex];
                                AggregateOnceBySegment(aggType, i, val);
                            }
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }

            private void AggregateOnceBySegment(AggregationType aggregation, int i, StatefulTimestampValue val)
            {
                switch (aggregation)
                {
                    case AggregationType.Min:
                        if (double.IsNaN(Values[i]))
                            Values[i] = val.Min;
                        else
                            Values[i] = Math.Min(Values[i], val.Min);
                        break;
                    case AggregationType.Max:
                        if (double.IsNaN(Values[i]))
                            Values[i] = val.Max;
                        else
                            Values[i] = Math.Max(Values[i], val.Max);
                        break;
                    case AggregationType.Sum:
                    case AggregationType.Average:
                    case AggregationType.Mean:
                        if (double.IsNaN(Values[i]))
                            Values[i] = 0;
                        Values[i] = Values[i] + val.Sum;
                        break;
                    case AggregationType.First:
                        if (double.IsNaN(Values[i]))
                            Values[i] = val.First;
                        break;
                    case AggregationType.Last:
                        Values[i] = val.Last;
                        break;
                    case AggregationType.Count:
                        if (double.IsNaN(Values[i]))
                            Values[i] = 0;
                        Values[i] += val.Count;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException("Unknown aggregation operation: " + aggregation);
                }
            }

            private void AggregateOnceByItem(AggregationType aggregation, int i, double val)
            {
                switch (aggregation)
                {
                    case AggregationType.Min:
                        if (double.IsNaN(Values[i]))
                            Values[i] = val;
                        else
                            Values[i] = Math.Min(Values[i], val);
                        break;
                    case AggregationType.Max:
                        if (double.IsNaN(Values[i]))
                            Values[i] = val;
                        else
                            Values[i] = Math.Max(Values[i], val);
                        break;
                    case AggregationType.Sum:
                    case AggregationType.Average:
                    case AggregationType.Mean:
                        if (double.IsNaN(Values[i]))
                            Values[i] = 0;
                        Values[i] = Values[i] + val;
                        break;
                    case AggregationType.First:
                        if (double.IsNaN(Values[i]))
                            Values[i] = val;
                        break;
                    case AggregationType.Last:
                        Values[i] = val;
                        break;
                    case AggregationType.Count:
                        if (double.IsNaN(Values[i]))
                            Values[i] = 0;
                        Values[i]++;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException("Unknown aggregation operation: " + aggregation);
                }
            }

            public void Step(Span<double> values)
            {
                EnsureNumberOfValues(values.Length);
                
                for (int i = 0; i < values.Length; i++)
                {
                    var val = values[i];
                    switch (_mode)
                    {
                        case AggregationMode.FromRaw:
                            for (var index = 0; index < Aggregations.Length; index++)
                            {
                                var aggregation = Aggregations[index];
                                var aggIndex = index + (i * Aggregations.Length);
                                AggregateOnceByItem(aggregation, aggIndex, val);
                            }

                            break;
                        case AggregationMode.FromAggregated:
                            {
                                var aggIndex = i % Aggregations.Length;
                                var type = Aggregations[aggIndex];
                                AggregateOnceByItem(type, i, val);
                            }
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }

            private void EnsureNumberOfValues(int numberOfValues)
            {
                switch (_mode)
                {
                    case AggregationMode.FromRaw:
                        var entries = numberOfValues * Aggregations.Length;
                        for (int i = Values.Count; i < entries; i++)
                        {
                            Values.Add(double.NaN);
                        }

                        break;
                    case AggregationMode.FromAggregated:
                        for (int i = Values.Count; i < numberOfValues; i++)
                        {
                            Values.Add(double.NaN);
                        }

                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
        }

        public static List<TimeSeriesStorage.Reader.SingleResult> GetAggregatedValues(TimeSeriesStorage.Reader reader, TimeSpan rangeGroup, AggregationMode mode)
        {
            var aggStates = new TimeSeriesAggregation(mode); // we always will aggregate here by Min, Max, First, Last, Sum, Count, Mean
            var results = new List<TimeSeriesStorage.Reader.SingleResult>();

            var rangeSpec = new TimeSeriesFunction.RangeGroup
            {
                Ticks = rangeGroup.Ticks
            };
            DateTime next = default;

            foreach (var it in reader.SegmentsOrValues())
            {
                if (it.IndividualValues != null)
                {
                    AggregateIndividualItems(it.IndividualValues);
                }
                else
                {
                    //We might need to close the old aggregation range and start a new one
                    MaybeMoveToNextRange(it.Segment.Start);

                    // now we need to see if we can consume the whole segment, or 
                    // if the range it cover needs to be broken up to multiple ranges.
                    // For example, if the segment covers 3 days, but we have group by 1 hour,
                    // we still have to deal with the individual values
                    if (it.Segment.End > next)
                    {
                        AggregateIndividualItems(it.Segment.Values);
                    }
                    else
                    {
                        var span = it.Segment.Summary.Span;
                        aggStates.Segment(span);
                    }
                }
            }

            if (aggStates.Any)
            {
                results.Add(new TimeSeriesStorage.Reader.SingleResult
                {
                    Timestamp = next.AddTicks(-1),
                    Values = new Memory<double>(aggStates.Values.ToArray()),
                    Status = TimeSeriesValuesSegment.Live,
                    // TODO: Tag = ""
                });
            }

            return results;

            void MaybeMoveToNextRange(DateTime ts)
            {
                if (ts < next)
                    return;

                if (aggStates.Any)
                {
                    results.Add(new TimeSeriesStorage.Reader.SingleResult
                    {
                        Timestamp = next.AddTicks(-1),
                        Values = new Memory<double>(aggStates.Values.ToArray()),
                        Status = TimeSeriesValuesSegment.Live,
                        // TODO: Tag = ""
                    });
                }

                var start = rangeSpec.GetRangeStart(ts);
                next = rangeSpec.GetNextRangeStart(start);
                aggStates.Init();
            }

            void AggregateIndividualItems(IEnumerable<TimeSeriesStorage.Reader.SingleResult> items)
            {
                foreach (var cur in items)
                {
                    if (cur.Status == TimeSeriesValuesSegment.Dead)
                        continue;

                    MaybeMoveToNextRange(cur.Timestamp);
                    
                    aggStates.Step(cur.Values.Span);
                }
            }
        }
        public static long NextRollup(DateTime time, TimeSeriesPolicy nextPolicy)
        {
            var integerPart = time.Ticks / nextPolicy.AggregationTime.Ticks;
            var nextRollup = nextPolicy.AggregationTime.Ticks * (integerPart + 1);
            return nextRollup;
        }
    }
}
