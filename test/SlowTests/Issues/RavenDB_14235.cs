using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Session;
using Raven.Server.Documents;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Threading;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14235 : RavenTestBase
    {
        private readonly List<int> _listOfNumsThatAreNulls = new List<int> { 9, 10, 11, 12, 13 };

        public RavenDB_14235(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public unsafe void PassingOnlyEscapedCharactersAsId1()
        {
            string str = "\t\t";
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            using (var allocator = new ByteStringContext(SharedMultipleUseFlag.None))
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(allocator, str, out var lowerIdSlice, out var idSlice))
            {
                var writer = new AsyncBlittableJsonTextWriter(ctx, new MemoryStream());
                var s1 = ctx.AllocateStringValue(null, lowerIdSlice.Content.Ptr, lowerIdSlice.Size);
                s1.LazyStringFormat = false;
                var s2 = ctx.GetLazyStringValue(idSlice.Content.Ptr);
                writer.WriteString(s1);
                writer.WriteString(s2);
            }
        }

        [Fact]
        public unsafe void PassingOnlyEscapedCharactersAsId2()
        {
            var bytes = new byte[24];
            bytes[0] = 0xef;
            bytes[1] = 0xbf;
            bytes[2] = 0xbd;
            bytes[3] = 0x33;
            bytes[4] = 0x13;
            bytes[5] = 0xef;
            bytes[6] = 0xbf;
            bytes[7] = 0xbd;
            bytes[8] = 0x27;
            bytes[9] = 0xca;
            bytes[10] = 0x83;
            bytes[11] = 0x32;
            bytes[12] = 0xef;
            bytes[13] = 0xbf;
            bytes[14] = 0xbd;
            bytes[15] = 0x1f;
            bytes[16] = 0xef;
            bytes[17] = 0xbf;
            bytes[18] = 0xbd;
            bytes[19] = 0xef;
            bytes[20] = 0xbf;
            bytes[21] = 0xbd;
            bytes[22] = 0x57;
            bytes[23] = 0x09;

            string str;

            unsafe
            {
                fixed (byte* ptr = bytes)
                {
                    str = Encoding.UTF8.GetString(ptr, bytes.Length);
                }
            }

            // str = "\u0080";
            var b = Encoding.UTF8.GetBytes(str);
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            using (var allocator = new ByteStringContext(SharedMultipleUseFlag.None))
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(allocator, str, out var lowerIdSlice, out var idSlice))
            {
                var writer = new AsyncBlittableJsonTextWriter(ctx, new MemoryStream());
                var s1 = ctx.AllocateStringValue(null, lowerIdSlice.Content.Ptr, lowerIdSlice.Size);
                var s2 = ctx.GetLazyStringValue(idSlice.Content.Ptr);
                writer.WriteString(s1);
                writer.WriteString(s2);
            }
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(4)]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(32)]
        [InlineData(64)]
        [InlineData(128)]
        [InlineData(256)]
        public async Task PassingOnlyEscapedCharactersAsId(int size)
        {
            char[] chars = new char[size];

            for (int c = 0; c < 32; c++)
            {
                if (c.In(_listOfNumsThatAreNulls))
                {
                    // id string created from 'only' those chars, will be identified as whitespace and will get replaced by guid.
                    continue;
                }

                for (int i = 0; i < size; i++)
                    chars[i] = (char)c;

                var bytes = new byte[24];
                bytes[0] = 0xef;
                bytes[1] = 0xbf;
                bytes[2] = 0xbd;
                bytes[3] = 0x33;
                bytes[4] = 0x13;
                bytes[5] = 0xef;
                bytes[6] = 0xbf;
                bytes[7] = 0xbd;
                bytes[8] = 0x27;
                bytes[9] = 0xca;
                bytes[10] = 0x83;
                bytes[11] = 0x32;
                bytes[12] = 0xef;
                bytes[13] = 0xbf;
                bytes[14] = 0xbd;
                bytes[15] = 0x1f;
                bytes[16] = 0xef;
                bytes[17] = 0xbf;
                bytes[18] = 0xbd;
                bytes[19] = 0xef;
                bytes[20] = 0xbf;
                bytes[21] = 0xbd;
                bytes[22] = 0x57;
                bytes[23] = 0x09;

                string str;

                unsafe
                {
                    fixed (byte* ptr = bytes)
                    {
                        str = Encoding.UTF8.GetString(ptr, bytes.Length);
                    }
                }
                // var encoded = Encoding.ASCII.GetString(bytes);
               
                // var str = "users/" + encoded;
               
                using (var store = GetDocumentStore())
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { WeirdName = str }, str);
                        await session.SaveChangesAsync();
                    }
                    WaitForUserToContinueTheTest(store, debug: false);
                    using (var session = store.OpenAsyncSession())
                    {
                        var u = await session.LoadAsync<User>(str);
                        var id = session.Advanced.GetDocumentId(u);

                        Assert.Equal(str, u.WeirdName);
                        Assert.Equal(str, id);
                    }
                }
            }
        }

        [Theory]
        [InlineData(4)]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(32)]
        [InlineData(64)]
        [InlineData(128)]
        [InlineData(256)]
        public async Task CombiningEscapedCharactersAsId(int size)
        {
            var partialSize = size / 4;
            const string abc = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            var magicNums = new List<int> { 92, 34, 8, 9, 10, 12, 13 };
            char[] chars = new char[partialSize];

            for (int c = 0; c < 32; c++)
            {
                for (int i = 0; i < partialSize; i++)
                    chars[i] = (char)c;

                var str = new string(chars);

                var random = new Random();
                str += new string(Enumerable.Repeat(abc, partialSize).Select(s => s[random.Next(s.Length)]).ToArray());

                for (int i = 0; i < partialSize; i++)
                    chars[i] = (char)magicNums[random.Next(0, magicNums.Count)];

                str += new string(chars);
                str += new string(Enumerable.Repeat(abc, partialSize).Select(s => s[random.Next(s.Length)]).ToArray());

                using (var store = GetDocumentStore())
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { WeirdName = str }, str);
                        await session.SaveChangesAsync();
                    }
                    using (var session = store.OpenAsyncSession())
                    {
                        var u = await session.LoadAsync<User>(str);
                        var id = session.Advanced.GetDocumentId(u);

                        Assert.Equal(str, u.WeirdName);
                        Assert.Equal(str, id);
                    }
                }
            }
        }

        [Theory]
        [InlineData(4)]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(32)]
        [InlineData(64)]
        [InlineData(128)]
        [InlineData(256)]
        public async Task CombiningEscapedCharactersAsCollectionName(int size)
        {
            var partialSize = size / 4;
            const string abc = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            var magicNums = new List<int> { 92, 34, 8, 9, 10, 12, 13 };
            char[] chars = new char[partialSize];

            for (int c = 0; c < 32; c++)
            {
                for (int i = 0; i < partialSize; i++)
                    chars[i] = (char)c;

                var str = new string(chars);

                var random = new Random();
                str += new string(Enumerable.Repeat(abc, partialSize).Select(s => s[random.Next(s.Length)]).ToArray());

                for (int i = 0; i < partialSize; i++)
                    chars[i] = (char)magicNums[random.Next(0, magicNums.Count)];

                str += new string(chars);
                str += new string(Enumerable.Repeat(abc, partialSize).Select(s => s[random.Next(s.Length)]).ToArray());

                using var store = GetDocumentStore();
                store.Commands().Put(str, null, new { WeirdName = str }, new Dictionary<string, object>
                {
                    { "@collection", str }
                });

                using var session = store.OpenAsyncSession();
                var u = await session.LoadAsync<User>(str);
                var id = session.Advanced.GetDocumentId(u);
                var collection = session.Advanced.GetMetadataFor(u)["@collection"];

                Assert.Equal(str, u.WeirdName);
                Assert.Equal(str, id);
                Assert.Equal(str, collection);
            }
        }

        [Fact]
        public async Task ShouldThrowOnNotAwaitedAsyncMethods()
        {
            // "Running this test makes that not awaited async calls (intentional) - we need to wait for tasks to avoid heap corruption. See https://issues.hibernatingrhinos.com/issue/RavenDB-16759
            const string name = "1";

            var exceptionsCounter = 0;
            var tryCounter = 0;
            using var store = GetDocumentStore();
            try
            {
                tryCounter++;
                using var session = store.OpenAsyncSession();
                var task = session.LoadAsync<User>(name);
                using (((InMemoryDocumentSessionOperations)session).ForTestingPurposesOnly().CallOnSessionDisposeAboutToThrowDueToRunningAsyncTask(() =>
                {
                    task.Wait();
                }))
                {

                }
            }
            catch (Exception e)
            {
                Assert.Equal(typeof(InvalidOperationException), e.GetType());
                exceptionsCounter++;
            }

            try
            {
                tryCounter++;
                using var session = store.OpenAsyncSession();
                await session.StoreAsync(new User());
                var task = session.SaveChangesAsync();
                using (((InMemoryDocumentSessionOperations)session).ForTestingPurposesOnly().CallOnSessionDisposeAboutToThrowDueToRunningAsyncTask(() =>
                {
                    task.Wait();
                }))
                {

                }
            }
            catch (Exception e)
            {
                Assert.Equal(typeof(InvalidOperationException), e.GetType());
                exceptionsCounter++;
            }

            try
            {
                tryCounter++;
                using var session = store.OpenAsyncSession();
                var task = session.Query<User>().ToListAsync();
                using (((InMemoryDocumentSessionOperations)session).ForTestingPurposesOnly().CallOnSessionDisposeAboutToThrowDueToRunningAsyncTask(() =>
                {
                    task.Wait();
                }))
                {

                }
            }
            catch (Exception e)
            {
                Assert.Equal(typeof(InvalidOperationException), e.GetType());
                exceptionsCounter++;
            }

            Task<AttachmentResult> getAttachmentTask = null;
            try
            {
                tryCounter++;
                using var session = store.OpenAsyncSession();
                getAttachmentTask = session.Advanced.Attachments.GetAsync(name, name);
                using (((InMemoryDocumentSessionOperations)session).ForTestingPurposesOnly().CallOnSessionDisposeAboutToThrowDueToRunningAsyncTask(() =>
                {
                    getAttachmentTask.Wait();
                }))
                {

                }
            }
            catch (Exception e)
            {
                Assert.Equal(typeof(InvalidOperationException), e.GetType());
                exceptionsCounter++;
            }
            finally
            {
                getAttachmentTask?.Result?.Dispose();
            }

            try
            {
                tryCounter++;
                using var session = store.OpenAsyncSession();
                var task = session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<object>(name);
                using (((InMemoryDocumentSessionOperations)session).ForTestingPurposesOnly().CallOnSessionDisposeAboutToThrowDueToRunningAsyncTask(() =>
                {
                    task.Wait();
                }))
                {

                }
            }
            catch (Exception e)
            {
                Assert.Equal(typeof(InvalidOperationException), e.GetType());
                exceptionsCounter++;
            }

            try
            {
                tryCounter++;
                using var session = store.OpenAsyncSession();
                var lazy = session.Advanced.Lazily.LoadAsync<User>(new[] { name, name, name, name });
                var task = lazy.Value;
                task.Start();
                using (((InMemoryDocumentSessionOperations)session).ForTestingPurposesOnly().CallOnSessionDisposeAboutToThrowDueToRunningAsyncTask(() =>
                {
                    task.Wait();
                }))
                {

                }
            }
            catch (Exception e)
            {
                Assert.Equal(typeof(InvalidOperationException), e.GetType());
                exceptionsCounter++;
            }

            Assert.True(tryCounter >= exceptionsCounter);
        }

        private class User
        {
            public string WeirdName { get; set; }
        }
    }
}
