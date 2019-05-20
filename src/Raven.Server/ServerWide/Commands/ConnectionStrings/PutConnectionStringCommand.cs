﻿using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ConnectionStrings
{
    public abstract class PutConnectionStringCommand<T> : UpdateDatabaseCommand where T : ConnectionString
    {
        public T ConnectionString { get; protected set; }

        protected PutConnectionStringCommand()
        {
            // for deserialization
        }

        protected PutConnectionStringCommand(T connectionString, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            ConnectionString = connectionString;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ConnectionString)] = ConnectionString.ToJson();
        }
    }

    public class PutRavenConnectionStringCommand : PutConnectionStringCommand<RavenConnectionString>
    {
        protected PutRavenConnectionStringCommand()
        {
            // for deserialization
        }

        public PutRavenConnectionStringCommand(RavenConnectionString connectionString, string databaseName, string uniqueRequestId) : base(connectionString, databaseName, uniqueRequestId)
        {
            
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.RavenConnectionStrings[ConnectionString.Name] = ConnectionString;
            return null;
        }
    }

    public class PutSqlConnectionStringCommand : PutConnectionStringCommand<SqlConnectionString>
    {
        protected PutSqlConnectionStringCommand()
        {
            // for deserialization
        }

        public PutSqlConnectionStringCommand(SqlConnectionString connectionString, string databaseName, string uniqueRequestId) : base(connectionString, databaseName, uniqueRequestId)
        {

        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.SqlConnectionStrings[ConnectionString.Name] = ConnectionString;
            return null;
        }
    }
}
