// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
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
            newValue.value = oldValue.value + input.value;
        }

        public int InitialValueLength(MyKey key, MyInput input)
        {
            return sizeof(int);
        }

        public void InitialUpdater(MyKey key, MyInput input, ref MyValue value)
        {
            value.value = input.value;
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
}
