using System.Net.Http;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Queries;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public sealed class QueriesHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/queries", "POST", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task Post()
        {
            using (var processor = new DatabaseQueriesHandlerProcessorForGet(this, HttpMethod.Post))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/queries", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true, Description = "Executes a RQL (Raven Query Language) query and returns matching documents. Supports projections, filtering, ordering, and includes.")]
        [RavenActionQueryParameter("query", true, "The RQL query string to execute.")]
        [RavenActionQueryParameter("start", false, "Number of results to skip for pagination.", Type = "int", DefaultValue = "0")]
        [RavenActionQueryParameter("pageSize", false, "Maximum number of results to return.", Type = "int")]
        [RavenActionQueryParameter("waitForNonStaleResults", false, "Wait for non-stale results before returning.", Type = "bool", DefaultValue = "false")]
        [RavenActionQueryParameter("waitForNonStaleResultsTimeout", false, "Maximum time to wait for non-stale results.", Type = "TimeSpan")]
        public async Task Get()
        {
            using (var processor = new DatabaseQueriesHandlerProcessorForGet(this, HttpMethod.Get))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/queries", "PATCH", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task Patch()
        {
            using (var processor = new DatabaseQueriesHandlerProcessorForPatch(this)) 
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/queries/test", "PATCH", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task PatchTest()
        {
            using (var processor = new DatabaseQueriesHandlerProcessorForPatchTest(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/queries", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Delete()
        {
            using (var processor = new DatabaseQueriesHandlerProcessorForDelete(this))
                await processor.ExecuteAsync();
        }
    }
}
