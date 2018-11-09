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

namespace FASTER.test
{
    public class MyKey
    {
        public int key;
        public MyKey Clone()
        {
            return this;
        }

        public long GetHashCode64()
        {
            return Utility.GetHashCode(key);
        }

        public bool Equals(MyKey otherKey)
        {
            return key == otherKey.key;
        }
        public void Serialize(Stream toStream)
        {
            new BinaryWriter(toStream).Write(key);
        }

        public void Deserialize(Stream fromStream)
        {
            key = new BinaryReader(fromStream).ReadInt32();
        }
    }

    public class MyValue
    {
        public int value;
        public MyValue Clone()
        {
            return this;
        }

        public void Serialize(Stream toStream)
        {
            new BinaryWriter(toStream).Write(value);
        }

        public void Deserialize(Stream fromStream)
        {
            value = new BinaryReader(fromStream).ReadInt32();
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

    public class MyFunctions : IUserFunctions<MyKey, MyValue, MyInput, MyOutput, MyContext>
    {
        public void RMWCompletionCallback(MyContext ctx, Status status)
        {
        }

        public void ReadCompletionCallback(MyContext ctx, MyOutput output, Status status)
        {
        }

        public void UpsertCompletionCallback(MyContext ctx)
        {
        }

        public void CopyUpdater(MyKey key, MyInput input, MyValue oldValue, ref MyValue newValue)
        {
            newValue = new MyValue { value = oldValue.value + input.value };
        }

        public int InitialValueLength(MyKey key, MyInput input)
        {
            return sizeof(int);
        }

        public void InitialUpdater(MyKey key, MyInput input, ref MyValue value)
        {
            value = new MyValue { value = input.value };
        }

        public void InPlaceUpdater(MyKey key, MyInput input, ref MyValue value)
        {
            value.value += input.value;
        }

        public void Reader(MyKey key, MyInput input, MyValue value, ref MyOutput dst)
        {
            dst.value = value;
        }
    }

    public class MyLargeValue
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
                value[i] = (byte)i;
            }
        }

        public MyLargeValue Clone()
        {
            return this;
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
    }

    public class MyLargeOutput
    {
        public MyLargeValue value;
    }

    public class MyLargeFunctions : IUserFunctions<MyKey, MyLargeValue, MyInput, MyLargeOutput, MyContext>
    {
        public void RMWCompletionCallback(MyContext ctx, Status status)
        {
        }

        public void ReadCompletionCallback(MyContext ctx, MyLargeOutput output, Status status)
        {
        }

        public void UpsertCompletionCallback(MyContext ctx)
        {
        }

        public void CopyUpdater(MyKey key, MyInput input, MyLargeValue oldValue, ref MyLargeValue newValue)
        {
        }

        public int InitialValueLength(MyKey key, MyInput input)
        {
            return sizeof(int);
        }

        public void InitialUpdater(MyKey key, MyInput input, ref MyLargeValue value)
        {
        }

        public void InPlaceUpdater(MyKey key, MyInput input, ref MyLargeValue value)
        {
        }

        public void Reader(MyKey key, MyInput input, MyLargeValue value, ref MyLargeOutput dst)
        {
            dst.value = value;
        }
    }
}
