// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ManagedSample3
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
        }

        public int InitialValueLength(MyKey key, MyInput input)
        {
            return sizeof(int) + sizeof(int);
        }

        public void InitialUpdater(MyKey key, MyInput input, ref MyValue value)
        {
        }

        public void InPlaceUpdater(MyKey key, MyInput input, ref MyValue value)
        {
        }

        public void Reader(MyKey key, MyInput input, MyValue value, ref MyOutput dst)
        {
            dst.value = value;
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var log = FasterFactory.CreateLogDevice(Path.GetTempPath() + "hybridlog.log");
            var h = FasterFactory.Create
                <MyKey, MyValue, MyInput, MyOutput, MyContext, MyFunctions>
                (128, log, new MyFunctions(),
                LogPageSizeBits: 10,
                LogTotalSizeBytes: 1L << 14
                );
              
            h.StartSession();

            for (int i = 0; i < 20000; i++)
            {
                h.Upsert(new MyKey { key = i }, new MyValue { value = i }, default(MyContext), 0);
                if (i % 32 == 0) h.Refresh();
            }
            MyOutput g1 = new MyOutput();
            h.Read(new MyKey { key = 23 }, new MyInput(), ref g1, new MyContext(), 0);

            h.CompletePending(true);

            MyOutput g2 = new MyOutput();
            h.Read(new MyKey { key = 46 }, new MyInput(), ref g2, new MyContext(), 0);
            h.CompletePending(true);

            Console.WriteLine("Success!");
            Console.ReadLine();
        }
    }
}
