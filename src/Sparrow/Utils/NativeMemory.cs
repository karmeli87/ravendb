﻿using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Collections.LockFree;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Sparrow.Platform.Win32;

namespace Sparrow.Utils
{
    public static unsafe class NativeMemory
    {
        private static float _minimumFreeCommittedMemory = 0.05f;

        public static readonly ThreadLocal<ThreadStats> ThreadAllocations = new ThreadLocal<ThreadStats>(
            () => new ThreadStats(), trackAllValues: true);

        public static void NotifyCurrentThreadAboutToClose()
        {
            ThreadAllocations.Value = null;
        }

        public static ConcurrentDictionary<string, ConcurrentDictionary<IntPtr, long>> FileMapping = new ConcurrentDictionary<string, ConcurrentDictionary<IntPtr, long>>();

        public static void SetMinimumFreeCommittedMemory(float min)
        {
            _minimumFreeCommittedMemory = min;
        }

        public class ThreadStats
        {
            public int Id;
            public int UnmanagedThreadId;
            public long Allocations;
            public long ReleasesFromOtherThreads;
            public Thread ThreadInstance;
            public string Name => ThreadInstance.Name;

            public long TotalAllocated => Allocations - ReleasesFromOtherThreads;

            public ThreadStats()
            {
                ThreadInstance = Thread.CurrentThread;
                Id = ThreadInstance.ManagedThreadId;
                UnmanagedThreadId = PlatformDetails.GetCurrentThreadId();
            }
        }

        public static void Free(byte* ptr, long size, ThreadStats stats)
        {            
            Debug.Assert(ptr != null);

            var currentThreadValue = ThreadAllocations.Value;            

            if (currentThreadValue == stats)
            {
                currentThreadValue.Allocations -= size;
                FixupReleasesFromOtherThreads(currentThreadValue);
            }
            else
            {
                Interlocked.Add(ref stats.ReleasesFromOtherThreads, size);
            }
            Marshal.FreeHGlobal((IntPtr)ptr);
        }

        public static void Free(byte* ptr, long size)
        {
            Free(ptr, size, ThreadAllocations.Value);
        }

        public static byte* AllocateMemory(long size)
        {
            ThreadStats _;
            return AllocateMemory(size, out _);
        }

        public static byte* AllocateMemory(long size, out ThreadStats thread)
        {
            thread = ThreadAllocations.Value;

            // Allocating when there isn't enough commit charge available is dangerous, on Linux, the OOM
            // will try to kill us. On Windows, we might get into memory allocation failures that are not
            // fun, so let's try to avoid it explicitly.
            // This is not expected to be called frequently, since we are caching the memory used here

            var memInfo = MemoryInformation.GetMemoryInfo();
            var overage = memInfo.CurrentCommitCharge * _minimumFreeCommittedMemory;
            if (overage >= memInfo.TotalCommittableMemory)
            {
                throw new OutOfMemoryException($"The amount of available memory to commit on the system is low. Commit charge: {memInfo.CurrentCommitCharge} / {memInfo.TotalCommittableMemory}." +
                    $" To prevent the system running out of commit charge entirely, we'll avoid allocating any more memory, denying allocation for {new Size(size, SizeUnit.Bytes)}")
                {
                    Data = {
                        ["Recoverable"] = "Nope"// not use false here to avoid boxing :-)
                    }
                };
            }

            try
            {
                var ptr = (byte*)Marshal.AllocHGlobal((IntPtr)size).ToPointer();
                thread.Allocations += size;
                return ptr;
            }
            catch (OutOfMemoryException e)
            {
                return ThrowFailedToAllocate(size, thread, e);
            }
        }
        
        private static byte* ThrowFailedToAllocate(long size, ThreadStats thread, OutOfMemoryException e)
        {
            throw new OutOfMemoryException($"Failed to allocate additional {new Size(size, SizeUnit.Bytes)} to already allocated {new Size(thread.Allocations, SizeUnit.Bytes)}", e);
        }

        private static void FixupReleasesFromOtherThreads(ThreadStats thread)
        {
            var released = thread.ReleasesFromOtherThreads;
            if (released > 0)
            {
                thread.Allocations -= released;
                Interlocked.Add(ref thread.ReleasesFromOtherThreads, -released);
            }
        }

        public static void RegisterFileMapping(string name, IntPtr start, long size)
        {
            var mapping = FileMapping.GetOrAdd(name,_ => new ConcurrentDictionary<IntPtr, long>());
            mapping.TryAdd(start, size);
        }

        public static void UnregisterFileMapping(string name)
        {
            ConcurrentDictionary<IntPtr, long> value;
            FileMapping.TryRemove(name, out value);
        }

        public static void UnregisterFileMapping(string name, IntPtr start, long size)
        {
            ConcurrentDictionary<IntPtr, long> mapping;
            if (FileMapping.TryGetValue(name, out mapping) == false)
                return;

            long _;
            mapping.TryRemove(start, out _);
            if (mapping.Count == 0)
            {
                ConcurrentDictionary<IntPtr, long> value;
                if (FileMapping.TryRemove(name, out value))
                {
                    if (value.Count > 0) // this shouldn't happen, but let us be on the safe side...
                    {
                        FileMapping.TryAdd(name, value);
                    }
                }
            }
        }

        public static byte* Allocate4KbAlignedMemory(long size, out ThreadStats thread)
        {
            Debug.Assert(size >= 0);

            thread = ThreadAllocations.Value;
            thread.Allocations += size;

            if (PlatformDetails.RunningOnPosix)
            {
                byte* ptr;
                var rc = Syscall.posix_memalign(&ptr, (IntPtr)4096, (IntPtr)size);
                if (rc != 0)
                    Syscall.ThrowLastError(rc, "Could not allocate memory");

                return ptr;
            }

            var allocate4KbAllignedMemory = Win32MemoryProtectMethods.VirtualAlloc(null, (UIntPtr)size, Win32MemoryProtectMethods.AllocationType.COMMIT,
                Win32MemoryProtectMethods.MemoryProtection.READWRITE);

            if (allocate4KbAllignedMemory == null)
                ThrowFailedToAllocate();

            return allocate4KbAllignedMemory;
        }

        private static void ThrowFailedToAllocate()
        {
            throw new Win32Exception("Could not allocate memory");
        }

        public static void Free4KbAlignedMemory(byte* ptr, int size, ThreadStats stats)
        {
            Debug.Assert(ptr != null);

            var currentThreadValue = ThreadAllocations.Value;
            if (currentThreadValue == stats)
            {
                currentThreadValue.Allocations -= size;
                FixupReleasesFromOtherThreads(currentThreadValue);
            }
            else
            {
                Interlocked.Add(ref stats.ReleasesFromOtherThreads, size);
            }

            var p = new IntPtr(ptr);
            if (PlatformDetails.RunningOnPosix)
            {
                Syscall.free(p);
                return;
            }

            if (Win32MemoryProtectMethods.VirtualFree(ptr, UIntPtr.Zero, Win32MemoryProtectMethods.FreeType.MEM_RELEASE) == false)
                ThrowFailedToFree();
        }

        private static void ThrowFailedToFree()
        {
            throw new Win32Exception("Failed to free memory");
        }

        public static void EnsureRegistered()
        {
            GC.KeepAlive(ThreadAllocations.Value); // side affecty
        }
    }
}
