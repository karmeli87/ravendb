﻿using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class StartIndexingOperation : IMaintenanceOperation
    {
        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new StartIndexingCommand();
        }

        internal class StartIndexingCommand : RavenCommand
        {
            private readonly string _type;

            public StartIndexingCommand()
            {
            }

            /// <summary>
            /// For Studio use only
            /// </summary>
            internal StartIndexingCommand(string type)
            {
                _type = type ?? throw new ArgumentNullException(nameof(type));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/indexes/start";

                if (_type != null)
                    url += $"?type={_type}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }
        }
    }
}
