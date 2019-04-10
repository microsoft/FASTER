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
    public struct Key : IFasterEqualityComparer<Key>, IVarLenStruct<Key>
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

        public int GetAverageLength()
        {
            return sizeof(long);
        }

        public int GetInitialLength<Input>(ref Input input)
        {
            return sizeof(long);
        }
    }

    [StructLayout(LayoutKind.Explicit)]
    public unsafe struct VLValue : IVarLenStruct<VLValue>
    {
        [FieldOffset(0)]
        public int length;

        public int GetAverageLength()
        {
            return sizeof(int) + 8;
        }

        public int GetInitialLength<Input>(ref Input input)
        {
            return sizeof(int) + 8;
        }

        public int GetLength(ref VLValue t)
        {
            return sizeof(int) + t.length;
        }

        public void ToByteArray(ref byte[] dst)
        {
            dst = new byte[length];
            byte* src = (byte*)Unsafe.AsPointer(ref this) + 4;
            for (int i=0; i<length; i++)
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
    }

    public struct Input
    {
        public long input;
    }

    public class VLFunctions : IFunctions<Key, VLValue, Input, byte[], Empty>
    {
        public void RMWCompletionCallback(ref Key key, ref Input input, Empty ctx, Status status)
        {
            Assert.IsTrue(status == Status.OK);
        }

        public void ReadCompletionCallback(ref Key key, ref Input input, ref byte[] output, Empty ctx, Status status)
        {
            Assert.IsTrue(status == Status.OK);
            for (int i=0; i<output.Length; i++)
            {
                Assert.IsTrue(output[i] == (byte)output.Length);
            }
        }

        public void UpsertCompletionCallback(ref Key key, ref VLValue output, Empty ctx)
        {
        }

        public void DeleteCompletionCallback(ref Key key, Empty ctx)
        {
        }

        public void CheckpointCompletionCallback(Guid sessionId, long serialNum)
        {
            Debug.WriteLine("Session {0} reports persistence until {1}", sessionId, serialNum);
        }

        // Read functions
        public void SingleReader(ref Key key, ref Input input, ref VLValue value, ref byte[] dst)
        {
            value.ToByteArray(ref dst);
        }

        public void ConcurrentReader(ref Key key, ref Input input, ref VLValue value, ref byte[] dst)
        {
            value.ToByteArray(ref dst);
        }

        // Upsert functions
        public void SingleWriter(ref Key key, ref VLValue src, ref VLValue dst)
        {
            src.CopyTo(ref dst);
        }

        public void ConcurrentWriter(ref Key key, ref VLValue src, ref VLValue dst)
        {
            src.CopyTo(ref dst);
        }

        // RMW functions
        public void InitialUpdater(ref Key key, ref Input input, ref VLValue value)
        {
        }

        public void InPlaceUpdater(ref Key key, ref Input input, ref VLValue value)
        {
        }

        public void CopyUpdater(ref Key key, ref Input input, ref VLValue oldValue, ref VLValue newValue)
        {
        }
    }

    public class VLFunctions2 : IFunctions<VLValue, VLValue, Input, byte[], Empty>
    {
        public void RMWCompletionCallback(ref VLValue key, ref Input input, Empty ctx, Status status)
        {
            Assert.IsTrue(status == Status.OK);
        }

        public void ReadCompletionCallback(ref VLValue key, ref Input input, ref byte[] output, Empty ctx, Status status)
        {
            Assert.IsTrue(status == Status.OK);
            for (int i = 0; i < output.Length; i++)
            {
                Assert.IsTrue(output[i] == (byte)output.Length);
            }
        }

        public void UpsertCompletionCallback(ref VLValue key, ref VLValue output, Empty ctx)
        {
        }

        public void DeleteCompletionCallback(ref VLValue key, Empty ctx)
        {
        }

        public void CheckpointCompletionCallback(Guid sessionId, long serialNum)
        {
            Debug.WriteLine("Session {0} reports persistence until {1}", sessionId, serialNum);
        }

        // Read functions
        public void SingleReader(ref VLValue key, ref Input input, ref VLValue value, ref byte[] dst)
        {
            value.ToByteArray(ref dst);
        }

        public void ConcurrentReader(ref VLValue key, ref Input input, ref VLValue value, ref byte[] dst)
        {
            value.ToByteArray(ref dst);
        }

        // Upsert functions
        public void SingleWriter(ref VLValue key, ref VLValue src, ref VLValue dst)
        {
            src.CopyTo(ref dst);
        }

        public void ConcurrentWriter(ref VLValue key, ref VLValue src, ref VLValue dst)
        {
            src.CopyTo(ref dst);
        }

        // RMW functions
        public void InitialUpdater(ref VLValue key, ref Input input, ref VLValue value)
        {
        }

        public void InPlaceUpdater(ref VLValue key, ref Input input, ref VLValue value)
        {
        }

        public void CopyUpdater(ref VLValue key, ref Input input, ref VLValue oldValue, ref VLValue newValue)
        {
        }
    }
}
