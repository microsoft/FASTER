// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

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

namespace FASTER.test
{
    public class MyKey : IKey<MyKey>
    {
        public int key;

        public long GetHashCode64()
        {
            return Utility.GetHashCode(key);
        }

        public void Serialize(Stream toStream)
        {
            new BinaryWriter(toStream).Write(key);
        }

        public void Deserialize(Stream fromStream)
        {
            key = new BinaryReader(fromStream).ReadInt32();
        }

        public int GetLength()
        {
            return 8;
        }

        public void ShallowCopy(ref MyKey dst)
        {
            dst = new MyKey { key = key };
        }

        public void Free()
        {
        }

        public bool Equals(ref MyKey k2)
        {
            return key == k2.key;
        }

        public bool HasObjectsToSerialize()
        {
            return true;
        }
    }

    public class MyValue : IValue<MyValue>
    {
        public int value;

        public void Serialize(Stream toStream)
        {
            new BinaryWriter(toStream).Write(value);
        }

        public void Deserialize(Stream fromStream)
        {
            value = new BinaryReader(fromStream).ReadInt32();
        }

        public int GetLength()
        {
            return 8;
        }

        public void ShallowCopy(ref MyValue dst)
        {
            dst.value = value;
        }

        public void Free()
        {
        }

        public void AcquireReadLock()
        {
        }

        public void AcquireWriteLock()
        {
        }

        public void ReleaseReadLock()
        {
        }

        public void ReleaseWriteLock()
        {
        }

        public bool HasObjectsToSerialize()
        {
            return true;
        }
    }
    
    public class MyInput
    {
        public int value;
    }

    public class MyOutput
    {
        public MyValue value;
    }


    public class MyContext
    {
    }

    public class MyFunctions : IFunctions<MyKey, MyValue, MyInput, MyOutput, MyContext>
    {
        public void InitialUpdater(ref MyKey key, ref MyInput input, ref MyValue value)
        {
            value = new MyValue { value = input.value };
        }

        public void InPlaceUpdater(ref MyKey key, ref MyInput input, ref MyValue value)
        {
            value.value += input.value;
        }

        public void CopyUpdater(ref MyKey key, ref MyInput input, ref MyValue oldValue, ref MyValue newValue)
        {
            newValue = new MyValue { value = oldValue.value + input.value };
        }

        public int InitialValueLength(ref MyKey key, ref MyInput input)
        {
            return sizeof(int);
        }

        public void ConcurrentReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst)
        {
            dst.value = value;
        }

        public void ConcurrentWriter(ref MyKey key, ref MyValue src, ref MyValue dst)
        {
            dst.value = src.value;
        }

        public void PersistenceCallback(long thread_id, long serial_num)
        {
        }

        public void ReadCompletionCallback(ref MyKey key, ref MyInput input, ref MyOutput output, ref MyContext ctx, Status status)
        {
        }

        public void RMWCompletionCallback(ref MyKey key, ref MyInput input, ref MyContext ctx, Status status)
        {
        }

        public void UpsertCompletionCallback(ref MyKey key, ref MyValue value, ref MyContext ctx)
        {
        }

        public void SingleReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst)
        {
            dst.value = value;
        }

        public void SingleWriter(ref MyKey key, ref MyValue src, ref MyValue dst)
        {
            dst = src;
        }
    }

    public class MyLargeValue : IValue<MyLargeValue>
    {
        public byte[] value;

        public MyLargeValue()
        {

        }

        public MyLargeValue(int size)
        {
            value = new byte[size];
            for (int i = 0; i < size; i++)
            {
                value[i] = (byte)(size+i);
            }
        }

        public void Serialize(Stream toStream)
        {
            var writer = new BinaryWriter(toStream);
            writer.Write(value.Length);
            writer.Write(value);
        }

        public void Deserialize(Stream fromStream)
        {
            var reader = new BinaryReader(fromStream);
            int size = reader.ReadInt32();
            value = reader.ReadBytes(size);
        }

        public int GetLength()
        {
            return 8;
        }

        public void ShallowCopy(ref MyLargeValue dst)
        {
            dst = this;
        }

        public void Free()
        {
        }

        public void AcquireReadLock()
        {
        }

        public void AcquireWriteLock()
        {
        }

        public void ReleaseReadLock()
        {
        }

        public void ReleaseWriteLock()
        {
        }

        public bool HasObjectsToSerialize()
        {
            return true;
        }
    }

    public class MyLargeOutput
    {
        public MyLargeValue value;
    }

    public class MyLargeFunctions : IFunctions<MyKey, MyLargeValue, MyInput, MyLargeOutput, MyContext>
    {
        public void RMWCompletionCallback(ref MyKey key, ref MyInput input, ref MyContext ctx, Status status)
        {
        }

        public void ReadCompletionCallback(ref MyKey key, ref MyInput input, ref MyLargeOutput output, ref MyContext ctx, Status status)
        {
            Assert.IsTrue(status == Status.OK);
            for (int i = 0; i < output.value.value.Length; i++)
            {
                Assert.IsTrue(output.value.value[i] == (byte)(output.value.value.Length+i));
            }
        }


        public void UpsertCompletionCallback(ref MyKey key, ref MyLargeValue value, ref MyContext ctx)
        {
        }

        public void CopyUpdater(ref MyKey key, ref MyInput input, ref MyLargeValue oldValue, ref MyLargeValue newValue)
        {
        }

        public int InitialValueLength(ref MyKey key, ref MyInput input)
        {
            return sizeof(int);
        }

        public void InitialUpdater(ref MyKey key, ref MyInput input, ref MyLargeValue value)
        {
        }

        public void InPlaceUpdater(ref MyKey key, ref MyInput input, ref MyLargeValue value)
        {
        }

        public void SingleReader(ref MyKey key, ref MyInput input, ref MyLargeValue value, ref MyLargeOutput dst)
        {
            dst.value = value;
        }

        public void ConcurrentReader(ref MyKey key, ref MyInput input, ref MyLargeValue value, ref MyLargeOutput dst)
        {
            dst.value = value;
        }

        public void ConcurrentWriter(ref MyKey key, ref MyLargeValue src, ref MyLargeValue dst)
        {
            dst = src;
        }

        public void PersistenceCallback(long thread_id, long serial_num)
        {
        }

        public void SingleWriter(ref MyKey key, ref MyLargeValue src, ref MyLargeValue dst)
        {
            dst = src;
        }
    }
}
