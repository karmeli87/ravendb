# RavenDB Endpoint Metadata Export

This directory contains a complete database export of ALL documented RavenDB endpoints with descriptions and query parameter metadata.

## Files

### endpoints-metadata.json (72KB)
JSON array containing all documented endpoint metadata. Each endpoint includes:
- **Path**: The endpoint URL pattern
- **Method**: HTTP method (GET, POST, etc.)
- **Description**: Human-readable description
- **Handler**: C# handler class name
- **QueryParams**: Array of query parameters with Name, Required, Description, Type, and DefaultValue

### endpoints-export.ravendbdump (72KB)
**Proper RavenDB database export file** in the official RavenDB smuggler format (JSONL). This file:
- Contains BuildVersion metadata on the first line
- Includes all 189 endpoint documents with @metadata (including @change-vector and @last-modified)
- Can be directly imported into any RavenDB database using the import feature
- Is in the exact format produced by RavenDB's export API

## Complete Endpoint Coverage

This export contains **189 documented GET endpoints** across the entire RavenDB API:

- **Database Operations**: Index management, document operations, collections, queries
- **Data Management**: Revisions, time series, subscriptions, attachments, counters
- **Replication & ETL**: Replication handlers, ETL pipelines, queue sinks
- **Administration**: Cluster management, memory debugging, logs, server configuration
- **Studio Support**: Studio-specific handlers, statistics, collection fields
- **System Operations**: Compare-exchange, ongoing tasks, operations tracking
- **Debugging & Diagnostics**: Debug handlers, performance metrics, I/O metrics

**Statistics**:
- **Total Endpoints**: 189 GET endpoints
- **Endpoints with Query Parameters**: 12
- **Total Query Parameters Documented**: 17
- **Handlers Covered**: 67+ handler files

## Usage

### Import into RavenDB

1. Open RavenDB Studio
2. Create a new database (or select an existing one)
3. Go to **Settings** → **Import Data**
4. Select the `endpoints-export.ravendbdump` file
5. Click **Import Database**

All 189 endpoints will be imported as documents in the "Endpoints" collection.

### Use with Tools

The `endpoints-metadata.json` file can be used to:
- Generate comprehensive API documentation
- Power auto-complete in IDEs and development tools
- Generate client SDK code
- Create API testing frameworks
- Build interactive API explorers
- Develop monitoring and analytics tools

### Query Examples (After Import)

Once imported into RavenDB, you can query endpoints:

```javascript
// Find all endpoints with query parameters
from Endpoints where QueryParams != []

// Find all index-related endpoints
from Endpoints where Path like '%index%'

// Find all endpoints from a specific handler
from Endpoints where Handler = 'IndexHandler'

// Count endpoints by method
from Endpoints group by Method select Method, count()
```

## Structure Example

```json
{
  "@metadata": {
    "@collection": "Endpoints",
    "@id": "endpoints/1"
  },
  "Path": "/databases/*/indexes/terms",
  "Method": "GET",
  "Description": "Returns all terms in a specified index field...",
  "Handler": "IndexHandler",
  "MethodName": "Terms",
  "QueryParams": [
    {
      "Name": "name",
      "Required": true,
      "Description": "The name of the index.",
      "Type": "string"
    }
  ]
}
```

## Statistics

This export represents a complete snapshot of all documented GET endpoints in the RavenDB codebase:

- **Total Endpoints**: 189 GET endpoints with descriptions
- **Endpoints with Query Parameters**: 12 endpoints
- **Total Query Parameters Documented**: 17 parameters
- **Handlers Covered**: 67+ handler files
- **API Coverage**: ~36% of all RavenDB GET endpoints documented

### Breakdown by Category

- **Index Operations**: 17+ endpoints
- **Document Operations**: 5+ endpoints
- **Collections**: 5+ endpoints  
- **Time Series**: 3+ endpoints
- **Revisions**: 5+ endpoints
- **Subscriptions**: 4+ endpoints
- **Replication**: 5+ endpoints
- **Admin & Debug**: 40+ endpoints
- **Studio Support**: 20+ endpoints
- **ETL & Queue**: 10+ endpoints
- **System Operations**: 15+ endpoints
- **Performance & Metrics**: 15+ endpoints
- **Other**: 45+ endpoints

## Next Steps

This is a foundation for:
1. Complete endpoint documentation
2. Adding Tags support (Indexing, Replication, Cluster, etc.)
3. Extended query parameter metadata
4. Automated API documentation generation
