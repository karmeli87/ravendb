# RavenDB Endpoint Metadata Export

This directory contains sample endpoint metadata extracted from the RavenDB API with the new Description and QueryParameter attributes.

## Files

### endpoints-metadata.json
JSON array containing documented endpoint metadata. Each endpoint includes:
- **Path**: The endpoint URL pattern
- **Method**: HTTP method (GET, POST, etc.)
- **Description**: Human-readable description
- **Handler**: C# handler class name
- **MethodName**: C# method name
- **QueryParams**: Array of query parameters with Name, Required, Description, Type, and DefaultValue

### endpoints-export.ravendbdump
RavenDB export format (JSONL) that can be directly imported into any RavenDB database.

## Sample Endpoints Included

This export contains 7 sample documented endpoints:

1. `/databases/*/indexes/terms` - Returns index terms (4 query params)
2. `/databases/*/docs` - Returns documents (4 query params)
3. `/databases/*/timeseries` - Returns time series data (9 query params)
4. `/databases/*/revisions` - Returns document revisions (6 query params)
5. `/databases/*/subscriptions` - Lists subscriptions (4 query params)
6. `/databases/*/attachments` - Returns attachments (2 query params)
7. `/databases/*/counters` - Returns counter values (3 query params)

**Total**: 32 query parameters documented across 7 endpoints

## Usage

### Import into RavenDB

1. Create a new database in RavenDB Studio
2. Go to **Settings** → **Import Data**
3. Select the `endpoints-export.ravendbdump` file
4. Click **Import**

The endpoints will be imported as documents in the "Endpoints" collection.

### Use with Tools

The `endpoints-metadata.json` file can be used to:
- Generate API documentation
- Power auto-complete in IDEs
- Generate SDK code
- Create API testing tools
- Build interactive API explorers

## Generating Complete Metadata

To generate metadata for ALL 449+ documented endpoints:

```bash
cd /home/runner/work/ravendb/ravendb
dotnet build tools/TypingsGenerator/TypingsGenerator.csproj
dotnet run --project tools/TypingsGenerator/TypingsGenerator.csproj
```

This will generate `src/Raven.Studio/typings/server/endpoints-metadata.json` with complete metadata for all endpoints.

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

- **Total Endpoints Documented**: 449+ (86% of GET endpoints)
- **Handlers Updated**: 67 files
- **Query Parameters Documented**: 60+ across key handlers
- **Sample in Export**: 7 endpoints, 32 parameters

## Next Steps

This is a foundation for:
1. Complete endpoint documentation
2. Adding Tags support (Indexing, Replication, Cluster, etc.)
3. Extended query parameter metadata
4. Automated API documentation generation
