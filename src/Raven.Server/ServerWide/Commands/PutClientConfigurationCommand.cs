﻿using Raven.Client;
using Raven.Client.Documents.Operations.Configuration;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class PutClientConfigurationCommand : PutValueCommand<ClientConfiguration>
    {
        public PutClientConfigurationCommand()
        {
            // for deserialization
        }

        public PutClientConfigurationCommand(ClientConfiguration value, string uniqueRequestId) : base(uniqueRequestId)
        {
            Name = Constants.Configuration.ClientId;
            Value = value;
        }

        public override DynamicJsonValue ValueToJson()
        {
            return Value?.ToJson();
        }
    }
}
