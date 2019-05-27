using System;
using SlowTests.Client.Counters;
using SlowTests.Cluster;

namespace Tryouts
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            for (int i = 0; i < 123; i++)
            {
                Console.WriteLine(i);
                using (var test = new ClusterTransactionTests())
                {
                    test.CanCreateClusterTransactionRequest2().Wait();
                }
            }
        }
    }
}
