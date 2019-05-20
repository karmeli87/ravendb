using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class EditRevisionsConfigurationCommand : UpdateDatabaseCommand
    {
        public RevisionsConfiguration Configuration { get; protected set; }

        public void UpdateDatabaseRecord(DatabaseRecord databaseRecord)
        {
            databaseRecord.Revisions = Configuration;
        }

        public EditRevisionsConfigurationCommand()
        {
        }

        public EditRevisionsConfigurationCommand(RevisionsConfiguration configuration, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            Configuration = configuration;
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.Revisions = Configuration;
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }
    }
}
