using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Stats;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public sealed class StatsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/stats/essential", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, Description = "Returns essential database statistics including document count, index count, and storage size.")]
        public async Task EssentialStats()
        {
            using (var processor = new StatsHandlerProcessorForEssentialStats(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/stats/detailed", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, Description = "Returns detailed database statistics with comprehensive metrics and performance data.")]
        public async Task DetailedStats()
        {
            using (var processor = new StatsHandlerProcessorForGetDetailedDatabaseStatistics(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/stats", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true, Description = "Returns general database statistics and health information.")]
        public async Task Stats()
        {
            using (var processor = new StatsHandlerProcessorForGetDatabaseStatistics(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/healthcheck", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, Description = "Performs a health check on the database and returns its status.")]
        public async Task DatabaseHealthCheck()
        {
            Database.ForTestingPurposes?.HealthCheckHold?.WaitOne();

            using (var processor = new StatsHandlerProcessorForDatabaseHealthCheck(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/metrics", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, Description = "Returns performance metrics for the database including request rates and throughput.")]
        public async Task Metrics()
        {
            using (var processor = new StatsHandlerProcessorForGetMetrics(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/metrics/puts", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, Description = "Returns metrics specific to document write operations (puts).")]
        public async Task PutsMetrics()
        {
            using (var processor = new StatsHandlerProcessorForGetMetricsPuts(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/metrics/bytes", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, Description = "Returns metrics on data transfer in bytes for the database.")]
        public async Task BytesMetrics()
        {
            using (var processor = new StatsHandlerProcessorForGetMetricsBytes(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/validate-unused-ids", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ValidateUnusedIds()
        {
            using (var processor = new StatsHandlerProcessorForPostValidateUnusedIds(this))
                await processor.ExecuteAsync();
        }
    }
}
