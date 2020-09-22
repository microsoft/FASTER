// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using FASTER.core;
using System.Runtime.CompilerServices;
using System.IO;
using System.Diagnostics;
using NUnit.Framework;
using System.Runtime.InteropServices;

namespace FASTER.test
{
    public struct Key : IFasterEqualityComparer<Key>, IVariableLengthStruct<Key>
    {
        public long key;

        public long GetHashCode64(ref Key key)
        {
            return Utility.GetHashCode(key.key);
        }
        public bool Equals(ref Key k1, ref Key k2)
        {
            return k1.key == k2.key;
        }

        public int GetLength(ref Key t)
        {
            return sizeof(long);
        }

        public int GetInitialLength()
        {
            return sizeof(long);
        }
    }

    [StructLayout(LayoutKind.Explicit)]
    public unsafe struct VLValue : IFasterEqualityComparer<VLValue>, IVariableLengthStruct<VLValue>
    {
        [FieldOffset(0)]
        public int length;

        [FieldOffset(4)]
        public int field1;

        public int GetInitialLength()
        {
            return 2 * sizeof(int);
        }

        public int GetLength(ref VLValue t)
        {
            return sizeof(int) * t.length;
        }

        public void ToIntArray(ref int[] dst)
        {
            dst = new int[length];
            int* src = (int*)Unsafe.AsPointer(ref this);
            for (int i = 0; i < length; i++)
            {
                dst[i] = *src;
                src++;
            }
        }

        public void CopyTo(ref VLValue dst)
        {
            var fulllength = GetLength(ref this);
            Buffer.MemoryCopy(Unsafe.AsPointer(ref this),
                Unsafe.AsPointer(ref dst), fulllength, fulllength);
        }

        public long GetHashCode64(ref VLValue k)
        {
            return Utility.GetHashCode(k.length) ^ Utility.GetHashCode(k.field1);
        }

        public bool Equals(ref VLValue k1, ref VLValue k2)
        {
            int* src = (int*)Unsafe.AsPointer(ref k1);
            int* dst = (int*)Unsafe.AsPointer(ref k2);
            int len = *src;

            for (int i = 0; i < len; i++)
                if (*(src + i) != *(dst + i))
                    return false;
            return true;
        }
    }

    public struct Input
    {
        public long input;
    }

    public class VLFunctions : IFunctions<Key, VLValue, Input, int[], Empty>
    {
        public void RMWCompletionCallback(ref Key key, ref Input input, Empty ctx, Status status)
        {
            Assert.IsTrue(status == Status.OK);
        }

        public void ReadCompletionCallback(ref Key key, ref Input input, ref int[] output, Empty ctx, Status status)
        {
            Assert.IsTrue(status == Status.OK);
            for (int i = 0; i < output.Length; i++)
            {
                Assert.IsTrue(output[i] == output.Length);
            }
        }

        public void UpsertCompletionCallback(ref Key key, ref VLValue output, Empty ctx)
        {
        }

        public void DeleteCompletionCallback(ref Key key, Empty ctx)
        {
        }

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
        {
            Debug.WriteLine("Session {0} reports persistence until {1}", sessionId, commitPoint.UntilSerialNo);
        }

        // Read functions
        public void SingleReader(ref Key key, ref Input input, ref VLValue value, ref int[] dst, ref Empty ctx)
        {
            value.ToIntArray(ref dst);
        }

        public void ConcurrentReader(ref Key key, ref Input input, ref VLValue value, ref int[] dst, ref Empty ctx)
        {
            value.ToIntArray(ref dst);
        }

        // Upsert functions
        public void SingleWriter(ref Key key, ref VLValue src, ref VLValue dst, ref Empty ctx)
        {
            src.CopyTo(ref dst);
        }

        public bool ConcurrentWriter(ref Key key, ref VLValue src, ref VLValue dst, ref Empty ctx)
        {
            if (src.length != dst.length)
                return false;

            src.CopyTo(ref dst);
            return true;
        }

        // RMW functions
        public void InitialUpdater(ref Key key, ref Input input, ref VLValue value, ref Empty ctx)
        {
        }

        public bool InPlaceUpdater(ref Key key, ref Input input, ref VLValue value, ref Empty ctx)
        {
            return true;
        }

        public void CopyUpdater(ref Key key, ref Input input, ref VLValue oldValue, ref VLValue newValue, ref Empty ctx)
        {
        }
    }

    public class VLFunctions2 : IFunctions<VLValue, VLValue, Input, int[], Empty>
    {
        public void RMWCompletionCallback(ref VLValue key, ref Input input, Empty ctx, Status status)
        {
            Assert.IsTrue(status == Status.OK);
        }

        public void ReadCompletionCallback(ref VLValue key, ref Input input, ref int[] output, Empty ctx, Status status)
        {
            Assert.IsTrue(status == Status.OK);
            for (int i = 0; i < output.Length; i++)
            {
                Assert.IsTrue(output[i] == output.Length);
            }
        }

        public void UpsertCompletionCallback(ref VLValue key, ref VLValue output, Empty ctx)
        {
        }

        public void DeleteCompletionCallback(ref VLValue key, Empty ctx)
        {
        }

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
        {
            Debug.WriteLine("Session {0} reports persistence until {1}", sessionId, commitPoint.UntilSerialNo);
        }

        // Read functions
        public void SingleReader(ref VLValue key, ref Input input, ref VLValue value, ref int[] dst, ref Empty ctx)
        {
            value.ToIntArray(ref dst);
        }

        public void ConcurrentReader(ref VLValue key, ref Input input, ref VLValue value, ref int[] dst, ref Empty ctx)
        {
            value.ToIntArray(ref dst);
        }

        // Upsert functions
        public void SingleWriter(ref VLValue key, ref VLValue src, ref VLValue dst, ref Empty ctx)
        {
            src.CopyTo(ref dst);
        }

        public bool ConcurrentWriter(ref VLValue key, ref VLValue src, ref VLValue dst, ref Empty ctx)
        {
            if (src.length != dst.length)
                return false;

            src.CopyTo(ref dst);
            return true;
        }

        // RMW functions
        public void InitialUpdater(ref VLValue key, ref Input input, ref VLValue value, ref Empty ctx)
        {
        }

        public bool InPlaceUpdater(ref VLValue key, ref Input input, ref VLValue value, ref Empty ctx)
        {
            return true;
        }

        public void CopyUpdater(ref VLValue key, ref Input input, ref VLValue oldValue, ref VLValue newValue, ref Empty ctx)
        {
        }
    }
}
