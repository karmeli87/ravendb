using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Esprima.Ast;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Server.Replication
{
    public class PullExternalReplicationTests : ReplicationTestBase
    {
        [Fact]
        public async Task PullExternalReplicationShouldWork()
        {
            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_FooBar-1",
                
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_FooBar-2"
            }))
            {
                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User(), "foo/bar");
                    s2.SaveChanges();
                }

                await SetupPullReplicationAsync(store1, store2);

                var timeout = 3000;
                Assert.True(WaitForDocument(store1, "foo/bar", timeout), store1.Identifier);
            }
        }

        [Fact]
        public async Task PullExternalReplicationShouldWorkWithCertificate()
        {
            var clusterSize = 3;

            var central = await CreateRaftClusterAndGetLeader(clusterSize, useSsl: true);
            var centralAdminCertPath = _selfSignedCertFileName;
            var minion = await CreateRaftClusterAndGetLeader(clusterSize, useSsl: true);
            var minionAdminCertPath = _selfSignedCertFileName;

            var centralAdmin = AskServerForClientCertificate(centralAdminCertPath, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: central);
            var minionAdmin = AskServerForClientCertificate(minionAdminCertPath, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: minion);

            var centralDB = GetDatabaseName();
            var minionDB = GetDatabaseName();

            X509Certificate2 pullReplicationCertificate;
            using (var store = new DocumentStore()
            {
                Urls = new[] { central.WebUrl },
                Database = centralDB,
                Certificate = centralAdmin,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                var requestExecutor = store.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new CreateClientCertificateOperation("client certificate", new Dictionary<string, DatabaseAccess>
                        {
                            [centralDB] = DatabaseAccess.Limited
                        }, SecurityClearance.ValidUser)
                        .GetCommand(store.Conventions, context);

                    requestExecutor.Execute(command, context);
                    using (var archive = new ZipArchive(new MemoryStream(command.Result.RawData)))
                    {
                        var entry = archive.Entries.First(e => string.Equals(Path.GetExtension(e.Name), ".pfx", StringComparison.OrdinalIgnoreCase));
                        using (var stream = entry.Open())
                        {
                            var destination = new MemoryStream();
                            stream.CopyTo(destination);
                            pullReplicationCertificate = new X509Certificate2(destination.ToArray(), (string)null, X509KeyStorageFlags.MachineKeySet);
                        }
                    }
                }
            }

            await CreateDatabaseInCluster(centralDB, clusterSize, central.WebUrl, centralAdmin);
            await CreateDatabaseInCluster(minionDB, clusterSize, minion.WebUrl, minionAdmin);

            using (var centralStore = new DocumentStore
            {
                Urls = new[] { central.WebUrl },
                Database = centralDB,
                Certificate = centralAdmin
            }.Initialize())
            using (var minionStore = new DocumentStore
            {
                Urls = new[] {minion.WebUrl},
                Database = minionDB,
                Certificate = minionAdmin
            }.Initialize())
            {
                using (var session = centralStore.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(10), replicas: clusterSize - 1);
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                var requestExecutor = minionStore.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new PutClientCertificateOperation($"pull replication from {centralDB}", pullReplicationCertificate, new Dictionary<string, DatabaseAccess>
                    {
                        [centralDB] = DatabaseAccess.Limited
                    }, SecurityClearance.ValidUser).GetCommand(minionStore.Conventions, context);
                    requestExecutor.Execute(command, context);
                }

                var pullReplication = new ExternalReplication(centralDB,$"ConnectionString-{centralDB}")
                {
                    PullReplicationCertificate = pullReplicationCertificate.Thumbprint,
                    PullReplication = true,
                    MentorNode = "B", // this is the node were the data will be replicated to.
                };
                await AddWatcherToReplicationTopology((DocumentStore)minionStore, pullReplication, new[] {central.WebUrl});

                var user = WaitForDocumentToReplicate<User>(
                    minionStore,
                    "users/1",
                    30_000);

                Assert.NotNull(user);
                Assert.Equal("Karmel", user.Name);
            }
        }

        [Fact]
        public async Task CentralFailover()
        {
            var clusterSize = 3;
            var central = await CreateRaftClusterAndGetLeader(clusterSize);
            var minion = await CreateRaftClusterAndGetLeader(clusterSize);

            var centralDB = GetDatabaseName();
            var minionDB = GetDatabaseName();

            var dstTopology = await CreateDatabaseInCluster(minionDB, clusterSize, minion.WebUrl);
            var srcTopology = await CreateDatabaseInCluster(centralDB, clusterSize, central.WebUrl);

            using (var centralStore = new DocumentStore
            {
                Urls = new[] { central.WebUrl },
                Database = centralDB
            }.Initialize())
            using (var minionStore = new DocumentStore
            {
                Urls = new[] { minion.WebUrl },
                Database = minionDB
            }.Initialize())
            {
                using (var session = centralStore.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(10), replicas: clusterSize - 1);
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                // add pull replication with invalid discovery url to test the failover on database topology discovery
                var pullReplication = new ExternalReplication(centralDB, $"ConnectionString-{centralDB}")
                {
                    PullReplication = true,
                    MentorNode = "B", // this is the node were the data will be replicated to.
                };
                await AddWatcherToReplicationTopology((DocumentStore)minionStore, pullReplication, new[] { "http://127.0.0.1:1234", central.WebUrl });

                using (var dstSession = minionStore.OpenSession())
                {
                    Assert.True(await WaitForDocumentInClusterAsync<User>(
                        dstSession as DocumentSession,
                        "users/1",
                        u => u.Name.Equals("Karmel"),
                        TimeSpan.FromSeconds(30)));
                }

                var minionUrl = minion.ServerStore.GetClusterTopology().GetUrlFromTag("B");
                var server = Servers.Single(s => s.WebUrl == minionUrl);
                var handler = await InstantiateOutgoingTaskHandler(minionDB, server);
                Assert.True(WaitForValue(
                    () => handler.GetOngoingTasksInternal().OngoingTasksList.Single(t => t is OngoingTaskReplication).As<OngoingTaskReplication>().DestinationUrl !=
                          null,
                    true));

                var watcherTaskUrl = handler.GetOngoingTasksInternal().OngoingTasksList.Single(t => t is OngoingTaskReplication).As<OngoingTaskReplication>()
                    .DestinationUrl;

                // dispose the central node, from which we are currently pulling 
                DisposeServerAndWaitForFinishOfDisposal(Servers.Single(s => s.WebUrl == watcherTaskUrl));

                using (var session = centralStore.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(10), replicas: clusterSize - 2);
                    session.Store(new User
                    {
                        Name = "Karmel2"
                    }, "users/2");
                    session.SaveChanges();
                }

                using (var dstSession = minionStore.OpenSession())
                {
                    Assert.True(await WaitForDocumentInClusterAsync<User>(
                        dstSession as DocumentSession,
                        "users/2",
                        u => u.Name.Equals("Karmel2"),
                        TimeSpan.FromSeconds(30)));
                }
            }
        }

        [Fact]
        public async Task EdgeFailover()
        {
            var clusterSize = 3;
            var central = await CreateRaftClusterAndGetLeader(clusterSize);
            var minion = await CreateRaftClusterAndGetLeader(clusterSize);

            var centralDB = GetDatabaseName();
            var minionDB = GetDatabaseName();

            var dstTopology = await CreateDatabaseInCluster(minionDB, clusterSize, minion.WebUrl);
            var srcTopology = await CreateDatabaseInCluster(centralDB, clusterSize, central.WebUrl);

            using (var centralStore = new DocumentStore
            {
                Urls = new[] { central.WebUrl },
                Database = centralDB
            }.Initialize())
            using (var minionStore = new DocumentStore
            {
                Urls = new[] { minion.WebUrl },
                Database = minionDB
            }.Initialize())
            {
                using (var session = centralStore.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(10), replicas: clusterSize - 1);
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                // add pull replication with invalid discovery url to test the failover on database topology discovery
                var pullReplication = new ExternalReplication(centralDB, $"ConnectionString-{centralDB}")
                {
                    PullReplication = true,
                    MentorNode = "B", // this is the node were the data will be replicated to.
                };
                await AddWatcherToReplicationTopology((DocumentStore)minionStore, pullReplication, new[] { "http://127.0.0.1:1234", central.WebUrl });

                using (var dstSession = minionStore.OpenSession())
                {
                    Assert.True(await WaitForDocumentInClusterAsync<User>(
                        dstSession as DocumentSession,
                        "users/1",
                        u => u.Name.Equals("Karmel"),
                        TimeSpan.FromSeconds(30)));
                }

                var minionUrl = minion.ServerStore.GetClusterTopology().GetUrlFromTag("B");
                var server = Servers.Single(s => s.WebUrl == minionUrl);
                var handler = await InstantiateOutgoingTaskHandler(minionDB, server);
                Assert.True(WaitForValue(
                    () => handler.GetOngoingTasksInternal().OngoingTasksList.Single(t => t is OngoingTaskReplication).As<OngoingTaskReplication>().DestinationUrl !=
                          null,
                    true));

                // dispose the minion node.
                DisposeServerAndWaitForFinishOfDisposal(server);

                using (var session = centralStore.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(10), replicas: clusterSize - 2);
                    session.Store(new User
                    {
                        Name = "Karmel2"
                    }, "users/2");
                    session.SaveChanges();
                }

                var user = WaitForDocumentToReplicate<User>(
                    minionStore,
                    "users/2",
                    30_000);

                Assert.Equal("Karmel2", user.Name);
            }
        }

        public Task<List<ModifyOngoingTaskResult>> SetupPullReplicationAsync(DocumentStore edge, params DocumentStore[] central)
        {
            return SetupPullReplicationAsync(edge, null, central);
        }

        public async Task<List<ModifyOngoingTaskResult>> SetupPullReplicationAsync(DocumentStore edge, string thumbprint,params DocumentStore[] central)
        {
            var tasks = new List<Task<ModifyOngoingTaskResult>>();
            var resList = new List<ModifyOngoingTaskResult>();
            foreach (var store in central)
            {
                var databaseWatcher = new ExternalReplication(store.Database,$"ConnectionString-{store.Database}")
                {
                    PullReplication = true,
                    PullReplicationCertificate = thumbprint
                };
                ModifyReplicationDestination(databaseWatcher);
                tasks.Add(AddWatcherToReplicationTopology(edge, databaseWatcher, store.Urls));
            }
            await Task.WhenAll(tasks);
            foreach (var task in tasks)
            {
                resList.Add(await task);
            }
            return resList;
        }
    }
}
