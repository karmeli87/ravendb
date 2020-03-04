using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.ServerWide;
using Raven.Server.Background;
using Raven.Server.Documents.Expiration;
using Raven.Server.Extensions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Utils;

namespace Raven.Server.Documents.TimeSeries
{
    

    public class TimeSeriesPolicyRetentionRunner : BackgroundWorkBase
    {
        private class PolicyState
        {
            public string Id;
            
            public string Name;

            public string Collection;

            public long LastRolledupEtag;

            public DateTime StartFrom;

            public RollupPolicy Policy;
        }

        private readonly DocumentDatabase _database;

        private readonly DocumentsStorage _documentStorage;

        private readonly TimeSeriesStorage _timeSeriesStorage;
        public TimeSeriesConfiguration Configuration { get; }

        private AsyncManualResetEvent _hasChanges;


        public TimeSeriesPolicyRetentionRunner(DocumentDatabase database, TimeSeriesConfiguration configuration) : base(database.Name, database.DatabaseShutdown)
        {
            _database = database;
            _documentStorage = database.DocumentsStorage;
            _timeSeriesStorage = _documentStorage.TimeSeriesStorage;
            Configuration = configuration;
            _hasChanges = new AsyncManualResetEvent(Cts.Token);
        }

        public static TimeSeriesPolicyRetentionRunner LoadConfigurations(DocumentDatabase database, DatabaseRecord dbRecord, TimeSeriesPolicyRetentionRunner policyRunner)
        {
            try
            {
                if (dbRecord.TimeSeries == null)
                {
                    policyRunner?.Dispose();
                    return null;
                }

                if (policyRunner != null)
                {
                    // no changes
                    if (Equals(policyRunner.Configuration, dbRecord.TimeSeries))
                        return policyRunner;
                }

                policyRunner?.Dispose();
                var runner = new TimeSeriesPolicyRetentionRunner(database, dbRecord.TimeSeries);
                runner.Start();
                return runner;
            }
            catch (Exception e)
            {
                const string msg = "Cannot enable retention policy runner as the configuration record is not valid.";
                database.NotificationCenter.Add(AlertRaised.Create(
                    database.Name,
                    $"retention policy runner error in {database.Name}", msg,
                    AlertType.RevisionsConfigurationNotValid, NotificationSeverity.Error, database.Name));

                var logger = LoggingSource.Instance.GetLogger<TimeSeriesPolicyRetentionRunner>(database.Name);
                if (logger.IsOperationsEnabled)
                    logger.Operations(msg, e);

                return null;
            }
        }

        private SortedDictionary<long, List<PolicyState>> _stateByTime = new SortedDictionary<long, List<PolicyState>>();
        private readonly Dictionary<string, long> _lastEtagPerCollection = new Dictionary<string, long>();

        private void FillCollectionIfNeeded(DocumentsOperationContext context)
        {
            foreach (var collection in _documentStorage.GetCollections(context))
            {
                _lastEtagPerCollection.TryAdd(collection.Name, 0);
            }
        }

        private void Run(DocumentsOperationContext context, string collection)
        {
            var config = Configuration.Collections[collection];

            if (config.Disabled)
                return;

            var etag = _lastEtagPerCollection[collection];

            foreach (var segmentEntry in _timeSeriesStorage.GetTimeSeriesFrom(context, collection, etag, 1024))
            {
                etag = segmentEntry.Etag;
                var name = segmentEntry.Name.ToString();

                //TODO: mark to deletion

                var current = config.GetPolicy(name);
                if (current == null) // policy not found
                    // TODO: delete this timeseries?
                    continue;

                var nextPolicy = config.GetNextPolicy(current);
                if (nextPolicy == null) 
                    continue;  // shouldn't happened, this mean the current policy doesn't exists
                
                if (nextPolicy == RollupPolicy.AfterAllPolices)
                    continue; // this is the last policy
            }

            _lastEtagPerCollection[collection] = etag;
        }

        public void MarkForRollup(DocumentsOperationContext context, string id, string collection, string name, long etag, DateTime baseline)
        {
            var config = Configuration.Collections[collection];

            if (config.Disabled)
                return;

            var current = config.GetPolicy(name);
            if (current == null) // policy not found
                // TODO: delete this timeseries if not the raw?
                return;

            var nextPolicy = config.GetNextPolicy(current);
            if (nextPolicy == null) 
                return;  // shouldn't happened, this mean the current policy doesn't exists
            
            if (nextPolicy == RollupPolicy.AfterAllPolices)
                return; // this is the last policy

            var integerPart = baseline.Ticks / nextPolicy.AggregateBy.Ticks;
            var nextRollup = nextPolicy.AggregateBy.Ticks * (integerPart + 1);
            var list = _stateByTime.GetOrAdd(nextRollup);

            list.Add(new PolicyState
            {
                StartFrom = baseline,
                Id = id,
                Name = name,
                Policy = nextPolicy,
                Collection = collection
            });
        }
       
        protected override async Task DoWork()
        {
            while (Cts.IsCancellationRequested == false)
            {
                await WaitOrThrowOperationCanceled(TimeSpan.FromSeconds(1));

                var currentTime = _database.Time.GetUtcNow();

                await RunRollups();
            }
        }

        public async Task RunRollups()
        {
            try
            {
                DatabaseTopology topology;
                using (_database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
                using (serverContext.OpenReadTransaction())
                {
                    topology = _database.ServerStore.Cluster.ReadDatabaseTopology(serverContext, _database.Name);
                }

                var isFirstInTopology = string.Equals(topology.AllNodes.FirstOrDefault(), _database.ServerStore.NodeTag, StringComparison.OrdinalIgnoreCase);

                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    while (true)
                    {
                        context.Reset();
                        context.Renew();

                        using (context.OpenReadTransaction())
                        {
                            var command = new RollupTimeSeriesCommand();
                            await _database.TxMerger.Enqueue(command);

                            /*if (Logger.IsInfoEnabled)
                                    Logger.Info(
                                        $"Successfully {(forExpiration ? "deleted" : "refreshed")} {command.DeletionCount:#,#;;0} documents in {duration.ElapsedMilliseconds:#,#;;0} ms.");*/
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // this will stop processing
                throw;
            }
            catch (Exception e)
            {
                /*if (Logger.IsOperationsEnabled)
                        Logger.Operations($"Failed to {(forExpiration ? "delete" : "refresh")} documents on {_database.Name} which are older than {currentTime}", e);*/
            }
        }

        internal class RollupTimeSeriesCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly long _now;
            private SortedDictionary<long, List<PolicyState>> _stateByTime = new SortedDictionary<long, List<PolicyState>>();

            public RollupTimeSeriesCommand()
            {
                _now = DateTime.UtcNow.Ticks;
            }
            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var count = 0;
                var tss = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage;
                foreach (var list in _stateByTime.Where(kvp => kvp.Key < _now).Select(kvp => kvp.Value))
                {
                    foreach (var item in list)
                    {
                        count++;
                        var policy = item.Policy;

                        var reader = tss.GetReader(context, item.Id, item.Name, item.StartFrom, DateTime.MaxValue);
                        var values = tss.GetAggregatedValues(reader, DateTime.MinValue, policy.AggregateBy, policy.Type);
                        tss.AppendTimestamp(context, item.Id, item.Collection, policy.Name, values);
                    }
                }

                return count;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                throw new NotImplementedException();
            }
        }
    }
}
