// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;

namespace FASTER.core
{
    public interface IObjectSerializer<T>
    {
        void BeginSerialize(Stream stream);
        void Serialize(ref T obj);
        void EndSerialize();
        void BeginDeserialize(Stream stream);
        void Deserialize(ref T obj);
        void EndDeserialize();
    }

    /// <summary>
    /// Serializer base class for binary reader and writer
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class BinaryObjectSerializer<T> : IObjectSerializer<T>
    {
        protected BinaryReader reader;
        protected BinaryWriter writer;

        public void BeginDeserialize(Stream stream)
        {
            reader = new BinaryReader(stream);
        }

        public abstract void Deserialize(ref T obj);

        public void EndDeserialize()
        {
        }

        public void BeginSerialize(Stream stream)
        {
            writer = new BinaryWriter(stream);
        }

        public abstract void Serialize(ref T obj);

        public void EndSerialize()
        {
            writer.Dispose();
        }
    }
}