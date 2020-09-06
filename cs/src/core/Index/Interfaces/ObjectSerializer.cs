// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.core
{
    internal static class ObjectSerializer
    {
        public static Func<IObjectSerializer<T>> Get<T>()
        {
            if (typeof(T) == typeof(string))
                return () => (IObjectSerializer<T>)new StringBinaryObjectSerializer();
            else if (typeof(T) == typeof(byte[]))
                return () => (IObjectSerializer<T>)new ByteArrayBinaryObjectSerializer();
            else
                return () => new DataContractObjectSerializer<T>();
        }
    }

    internal class StringBinaryObjectSerializer : BinaryObjectSerializer<string>
    {
        public override void Deserialize(out string obj) => obj = reader.ReadString();
        public override void Serialize(ref string obj) => writer.Write(obj);
    }

    internal class ByteArrayBinaryObjectSerializer : BinaryObjectSerializer<byte[]>
    {
        public override void Deserialize(out byte[] obj) => obj = reader.ReadBytes(reader.ReadInt32());
        public override void Serialize(ref byte[] obj)
        {
            writer.Write(obj.Length);
            writer.Write(obj);
        }
    }
}
