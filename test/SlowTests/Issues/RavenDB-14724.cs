﻿using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14724 : RavenTestBase
    {
        public RavenDB_14724(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Revisions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task DeleteDocumentAndRevisions(Options options)
        {
            var user = new User { Name = "Raven" };
            var id = "user/1";
            using (var store = GetDocumentStore(options))
            {
                await RevisionsHelper.SetupRevisionsAsync(store);

                using (var session = store.OpenSession())
                {
                    session.Store(user, id);
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    user = await session.LoadAsync<User>(id);
                    user.Age = 10;
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal(DocumentFlags.HasRevisions.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                    var revisions = await session.Advanced.Revisions.GetForAsync<User>(id);
                    Assert.Equal(2, revisions.Count);

                    session.Delete(id);
                    await session.SaveChangesAsync();

                    var configuration = new RevisionsConfiguration()
                    {
                        Default = null
                    };
       
                    await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);
                }


                using (var session = store.OpenSession())
                {
                    session.Store(user, id);
                    session.SaveChanges();
                    await RevisionsHelper.SetupRevisionsAsync(store);
                }

                using (var session = store.OpenAsyncSession())
                {
                    user = await session.LoadAsync<User>(id);
                    var revisions = await session.Advanced.Revisions.GetForAsync<User>(id);
                    Assert.Equal(0, revisions.Count);
                }
            }
        }
    }
}
