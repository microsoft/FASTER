// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ManagedSample4
{
    
    public struct Wrap<T>
    {
        public T field;

        public new long GetHashCode()
        {
            return Utility.GetHashCode(field.GetHashCode());
        }
    }

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

    public class MyFunctions : IUserFunctions<Wrap<int>, Wrap<int>, Wrap<int>, Wrap<int>, MyContext>
    {
        public void RMWCompletionCallback(MyContext ctx, Status status)
        {
        }

        public void ReadCompletionCallback(MyContext ctx, Wrap<int> output, Status status)
        {
        }
        public void UpsertCompletionCallback(MyContext ctx)
        {
        }

        public void CopyUpdater(Wrap<int> key, Wrap<int> input, Wrap<int> oldValue, ref Wrap<int> newValue)
        {
            newValue.field = oldValue.field + input.field;
        }

        public int InitialValueLength(Wrap<int> key, Wrap<int> input)
        {
            return sizeof(int) + sizeof(int);
        }

        public void InitialUpdater(Wrap<int> key, Wrap<int> input, ref Wrap<int> value)
        {
            value.field = input.field;
        }

        public void InPlaceUpdater(Wrap<int> key, Wrap<int> input, ref Wrap<int> value)
        {
            value.field += input.field;
        }

        public void Reader(Wrap<int> key, Wrap<int> input, Wrap<int> value, ref Wrap<int> dst)
        {
            dst.field = value.field;
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var log = FasterFactory.CreateLogDevice(Path.GetTempPath() + "hybridlog.log");
            var h = FasterFactory.Create
                <Wrap<int>, Wrap<int>, Wrap<int>, Wrap<int>, MyContext, MyFunctions>
                (128, log, null, new MyFunctions(),
                LogPageSizeBits: 10,
                LogTotalSizeBytes: 1L << 14
                );

            h.StartSession();

            for (int i = 0; i <20000; i++)
            {
                h.RMW(new Wrap<int> { field = i }, new Wrap<int> { field = i }, default(MyContext), 0);
                h.RMW(new Wrap<int> { field = i }, new Wrap<int> { field = i }, default(MyContext), 0);
                if (i % 32 == 0) h.Refresh();
            }
            Wrap<int> g1 = new Wrap<int>();
            h.Read(new Wrap<int> { field = 19999 }, new Wrap<int>(), ref g1, new MyContext(), 0);

            h.CompletePending(true);

            Wrap<int> g2 = new Wrap<int>();
            h.Read(new Wrap<int> { field = 46 }, new Wrap<int>(), ref g2, new MyContext(), 0);
            h.CompletePending(true);

            Console.WriteLine("Success!");
            Console.ReadLine();
        }
    }
}
