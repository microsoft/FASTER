// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using FASTER.core;
using System.Runtime.CompilerServices;
using NUnit.Framework;
using System.Runtime.InteropServices;

namespace FASTER.test
{
    public struct Key : IFasterEqualityComparer<Key>, IVariableLengthStruct<Key>
    {
        public long key;

        public long GetHashCode64(ref Key key) => Utility.GetHashCode(key.key);
        
        public bool Equals(ref Key k1, ref Key k2) => k1.key == k2.key;

        public int GetLength(ref Key t) => sizeof(long);

        public int GetInitialLength() => sizeof(long);

        public unsafe void Serialize(ref Key source, void* destination)
            => Buffer.MemoryCopy(Unsafe.AsPointer(ref source), destination, GetLength(ref source), GetLength(ref source));

        public unsafe ref Key AsRef(void* source) => ref Unsafe.AsRef<Key>(source);
        public unsafe void Initialize(void* source, void* dest) { }

        public override string ToString() => this.key.ToString();
    }

    [StructLayout(LayoutKind.Explicit)]
    public unsafe struct VLValue : IFasterEqualityComparer<VLValue>, IVariableLengthStruct<VLValue>
    {
        [FieldOffset(0)]
        public int length;

        [FieldOffset(4)]
        public int field1;

        public int GetInitialLength() => 2 * sizeof(int);

        public int GetLength(ref VLValue t) => sizeof(int) * t.length;

        public unsafe void Serialize(ref VLValue source, void* destination)
            => Buffer.MemoryCopy(Unsafe.AsPointer(ref source), destination, GetLength(ref source), GetLength(ref source));

        public unsafe ref VLValue AsRef(void* source) => ref Unsafe.AsRef<VLValue>(source);

        public unsafe void Initialize(void* source, void* dest) { }

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
            Buffer.MemoryCopy(Unsafe.AsPointer(ref this), Unsafe.AsPointer(ref dst), fulllength, fulllength);
        }

        public long GetHashCode64(ref VLValue k) => Utility.GetHashCode(k.length) ^ Utility.GetHashCode(k.field1);

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

        public override string ToString() => $"len = {this.length}, field1 = {this.field1}";
    }

    public struct Input
    {
        public long input;

        public override string ToString() => this.input.ToString();
    }

    public class VLFunctions : FunctionsBase<Key, VLValue, Input, int[], Empty>
    {
        public override void RMWCompletionCallback(ref Key key, ref Input input, ref int[] output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.IsTrue(status.Found);
            Assert.IsTrue(status.CopyUpdatedRecord);
        }

        public override void ReadCompletionCallback(ref Key key, ref Input input, ref int[] output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.IsTrue(status.Found);
            for (int i = 0; i < output.Length; i++)
            {
                Assert.AreEqual(output.Length, output[i]);
            }
        }

        // Read functions
        public override bool SingleReader(ref Key key, ref Input input, ref VLValue value, ref int[] dst, ref RecordInfo recordInfo, ref ReadInfo readInfo)
        {
            value.ToIntArray(ref dst);
            return true;
        }

        public override bool ConcurrentReader(ref Key key, ref Input input, ref VLValue value, ref int[] dst, ref RecordInfo recordInfo, ref ReadInfo readInfo)
        {
            value.ToIntArray(ref dst);
            return true;
        }

        // Upsert functions
        public override void SingleWriter(ref Key key, ref Input input, ref VLValue src, ref VLValue dst, ref int[] output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            src.CopyTo(ref dst);
        }

        public override bool ConcurrentWriter(ref Key key, ref Input input, ref VLValue src, ref VLValue dst, ref int[] output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo)
        {
            if (src.length != dst.length)
                return false;

            src.CopyTo(ref dst);
            return true;
        }
    }

    public class VLFunctions2 : FunctionsBase<VLValue, VLValue, Input, int[], Empty>
    {
        public override void RMWCompletionCallback(ref VLValue key, ref Input input, ref int[] output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.IsTrue(status.Found);
            Assert.IsTrue(status.CopyUpdatedRecord);
        }

        public override void ReadCompletionCallback(ref VLValue key, ref Input input, ref int[] output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.IsTrue(status.Found);
            for (int i = 0; i < output.Length; i++)
            {
                Assert.AreEqual(output.Length, output[i]);
            }
        }

        // Read functions
        public override bool SingleReader(ref VLValue key, ref Input input, ref VLValue value, ref int[] dst, ref RecordInfo recordInfo, ref ReadInfo readInfo)
        {
            value.ToIntArray(ref dst);
            return true;
        }

        public override bool ConcurrentReader(ref VLValue key, ref Input input, ref VLValue value, ref int[] dst, ref RecordInfo recordInfo, ref ReadInfo readInfo)
        {
            value.ToIntArray(ref dst);
            return true;
        }

        // Upsert functions
        public override void SingleWriter(ref VLValue key, ref Input input, ref VLValue src, ref VLValue dst, ref int[] output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            src.CopyTo(ref dst);
        }

        public override bool ConcurrentWriter(ref VLValue key, ref Input input, ref VLValue src, ref VLValue dst, ref int[] output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo)
        {
            if (src.length != dst.length)
                return false;

            src.CopyTo(ref dst);
            return true;
        }
    }
}
