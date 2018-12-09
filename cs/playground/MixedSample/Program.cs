// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MixedSample
{
    public class MyKey : IKey<MyKey>
    {
        public int key;

        public long GetHashCode64()
        {
            return Utility.GetHashCode(key);
        }

        public bool Equals(ref MyKey otherKey)
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

        public int GetLength()
        {
            return 8;
        }

        public void ShallowCopy(ref MyKey dst)
        {
            dst = this;
        }

        public void Free()
        {
        }

        public bool HasObjectsToSerialize()
        {
            return true;
        }

        public ref MyKey MoveToContext(ref MyKey key)
        {
            throw new NotImplementedException();
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
            dst = this;
        }

        public void Free()
        {
            throw new NotImplementedException();
        }

        public bool HasObjectsToSerialize()
        {
            return true;
        }

        public ref MyValue MoveToContext(ref MyValue value)
        {
            throw new NotImplementedException();
        }
    }

    public class MyInput : IMoveToContext<MyInput>
    {
        public ref MyInput MoveToContext(ref MyInput input)
        {
            throw new NotImplementedException();
        }
    }

    public class MyOutput : IMoveToContext<MyOutput>
    {
        public MyValue value;

        public ref MyOutput MoveToContext(ref MyOutput input)
        {
            throw new NotImplementedException();
        }
    }


    public class MyContext : IMoveToContext<MyContext>
    {
        public ref MyContext MoveToContext(ref MyContext input)
        {
            throw new NotImplementedException();
        }
    }

    public class MyFunctions : IFunctions<MyKey, MyValue, MyInput, MyOutput, MyContext>
    {
        public void ConcurrentReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst)
        {
            throw new NotImplementedException();
        }

        public void ConcurrentWriter(ref MyKey key, ref MyValue src, ref MyValue dst)
        {
            throw new NotImplementedException();
        }

        public void CopyUpdater(ref MyKey key, ref MyInput input, ref MyValue oldValue, ref MyValue newValue)
        {
            throw new NotImplementedException();
        }

        public void InitialUpdater(ref MyKey key, ref MyInput input, ref MyValue value)
        {
            throw new NotImplementedException();
        }

        public int InitialValueLength(ref MyKey key, ref MyInput input)
        {
            throw new NotImplementedException();
        }

        public void InPlaceUpdater(ref MyKey key, ref MyInput input, ref MyValue value)
        {
            throw new NotImplementedException();
        }

        public void PersistenceCallback(long thread_id, long serial_num)
        {
            throw new NotImplementedException();
        }

        public void ReadCompletionCallback(ref MyKey key, ref MyInput input, ref MyOutput output, ref MyContext ctx, Status status)
        {
            throw new NotImplementedException();
        }

        public void RMWCompletionCallback(ref MyKey key, ref MyInput input, ref MyContext ctx, Status status)
        {
            throw new NotImplementedException();
        }

        public void SingleReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst)
        {
            throw new NotImplementedException();
        }

        public void SingleWriter(ref MyKey key, ref MyValue src, ref MyValue dst)
        {
            throw new NotImplementedException();
        }

        public void UpsertCompletionCallback(ref MyKey key, ref MyValue value, ref MyContext ctx)
        {
            throw new NotImplementedException();
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var log = FasterFactory.CreateLogDevice(Path.GetTempPath() + "hybridlog");
            var objlog = FasterFactory.CreateObjectLogDevice(Path.GetTempPath() + "hybridlog");

            var h = new FasterKV
                <MyKey, MyValue, MyInput, MyOutput, MyContext, MyFunctions>
                (128, new MyFunctions(),
                new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MemorySizeBits = 29 }
                );

            var context = default(MyContext);

            h.StartSession();

            for (int i = 0; i < 20000; i++)
            {
                var _key = new MyKey { key = i };
                var value = new MyValue { value = i };
                h.Upsert(ref _key, ref value, ref context, 0);
                if (i % 32 == 0) h.Refresh();
            }
            var key = new MyKey { key = 23 };
            var input = default(MyInput);
            MyOutput g1 = new MyOutput();
            h.Read(ref key, ref input, ref g1, ref context, 0);

            h.CompletePending(true);

            MyOutput g2 = new MyOutput();
            key = new MyKey { key = 46 };
            h.Read(ref key, ref input, ref g2, ref context, 0);
            h.CompletePending(true);

            Console.WriteLine("Success!");
            Console.ReadLine();
        }
    }
}
