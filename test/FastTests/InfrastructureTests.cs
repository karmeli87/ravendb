using System;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests
{
    public class InfrastructureTests : ClusterTestBase
    {
        public InfrastructureTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanPropagateException()
        {
            var ae = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                var cluster = await CreateRaftCluster(2);
                using (var store = GetDocumentStore(new Options{Server = cluster.Leader}))
                {
                    cluster.Leader.Dispose();
                    throw new InvalidOperationException(); // this is the real exception
                }
            });
        }
    }
}
