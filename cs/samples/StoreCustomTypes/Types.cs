// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace StoreCustomTypes
{
    public class MyKey : IFasterEqualityComparer<MyKey>
    {
        public int key;

        public long GetHashCode64(ref MyKey key)
        {
            return Utility.GetHashCode(key.key);
        }

        public bool Equals(ref MyKey key1, ref MyKey key2)
        {
            return key1.key == key2.key;
        }
    }
    public class MyKeySerializer : BinaryObjectSerializer<MyKey>
    {
        public override void Serialize(ref MyKey key)
        {
            writer.Write(key.key);
        }

        public override void Deserialize(out MyKey key)
        {
            key = new MyKey
            {
                key = reader.ReadInt32()
            };
        }
    }


    public class MyValue
    {
        public int value;
    }

    public class MyValueSerializer : BinaryObjectSerializer<MyValue>
    {
        public override void Serialize(ref MyValue value)
        {
            writer.Write(value.value);
        }

        public override void Deserialize(out MyValue value)
        {
            value = new MyValue
            {
                value = reader.ReadInt32()
            };
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

    public class MyContext { }
}
