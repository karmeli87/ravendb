using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Collections;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public sealed class CollectionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/collections/stats", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, Description = "Returns statistics for all collections in the database including document counts, size estimates, and last document etag per collection. Lightweight alternative to detailed stats.")]
        public async Task GetCollectionStats()
        {
            using (var processor = new CollectionsHandlerProcessorForGetCollectionStats(this, detailed: false))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/collections/stats/detailed", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true, Description = "Returns detailed statistics for all collections.")]
        public async Task GetDetailedCollectionStats()
        {
            using (var processor = new CollectionsHandlerProcessorForGetCollectionStats(this, detailed: true))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/collections/docs", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, Description = "Returns documents from a specified collection. Optimized for retrieving all documents of a specific type/collection.")]
        [RavenActionQueryParameter("name", true, "The collection name to retrieve documents from.")]
        [RavenActionQueryParameter("start", false, "Number of documents to skip for pagination.", Type = "int", DefaultValue = "0")]
        [RavenActionQueryParameter("pageSize", false, "Maximum number of documents to return.", Type = "int")]
        public async Task GetCollectionDocuments()
        {
            using (var processor = new CollectionsHandlerProcessorForGetCollectionDocuments(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/collections/last-change-vector", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, Description = "Returns the last change vector for a collection.")]
        public async Task GetLastDocumentChangeVectorForCollection()
        {
            using (var processor = new CollectionsHandlerProcessorForGetLastChangeVector(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/revisions/collections/stats", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, Description = "Returns statistics for document revisions grouped by collection.")]
        public async Task GetRevisionsStats()
        {
            using (var processor = new CollectionsHandlerProcessorForGetCollectionRevisionsStats(this))
                await processor.ExecuteAsync();
        }
    }
}
