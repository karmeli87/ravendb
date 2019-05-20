using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Sorters
{
    public class DeleteSorterCommand : UpdateDatabaseCommand
    {
        public string SorterName;

        public DeleteSorterCommand()
        {
            // for deserialization
        }

        public DeleteSorterCommand(string name, string databaseName, string uniqueRequestId)
            : base(databaseName, uniqueRequestId)
        {
            SorterName = name;
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.DeleteSorter(SorterName);
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(SorterName)] = SorterName;
        }
    }
}
