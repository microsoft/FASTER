// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;

namespace FASTER.core
{
    /// <summary>
    /// Object serializer interface
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IObjectSerializer<T>
    {
        /// <summary>
        /// Begin serialization to given stream
        /// </summary>
        /// <param name="stream"></param>
        void BeginSerialize(Stream stream);

        /// <summary>
        /// Serialize object
        /// </summary>
        /// <param name="obj"></param>
        void Serialize(ref T obj);

        /// <summary>
        /// End serialization to given stream
        /// </summary>
        void EndSerialize();

        /// <summary>
        /// Begin deserialization from given stream
        /// </summary>
        /// <param name="stream"></param>
        void BeginDeserialize(Stream stream);

        /// <summary>
        /// Deserialize object
        /// </summary>
        /// <param name="obj"></param>
        void Deserialize(ref T obj);

        /// <summary>
        /// End deserialization from given stream
        /// </summary>
        void EndDeserialize();
    }

    /// <summary>
    /// Serializer base class for binary reader and writer
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class BinaryObjectSerializer<T> : IObjectSerializer<T>
    {
        /// <summary>
        /// Binary reader
        /// </summary>
        protected BinaryReader reader;

        /// <summary>
        /// Binary writer
        /// </summary>
        protected BinaryWriter writer;

        /// <summary>
        /// Begin deserialization
        /// </summary>
        /// <param name="stream"></param>
        public void BeginDeserialize(Stream stream)
        {
            reader = new BinaryReader(stream);
        }

        /// <summary>
        /// Deserialize
        /// </summary>
        /// <param name="obj"></param>
        public abstract void Deserialize(ref T obj);

        /// <summary>
        /// End deserialize
        /// </summary>
        public void EndDeserialize()
        {
        }

        /// <summary>
        /// Begin serialize
        /// </summary>
        /// <param name="stream"></param>
        public void BeginSerialize(Stream stream)
        {
            writer = new BinaryWriter(stream);
        }

        /// <summary>
        /// Serialize
        /// </summary>
        /// <param name="obj"></param>
        public abstract void Serialize(ref T obj);

        /// <summary>
        /// End serialize
        /// </summary>
        public void EndSerialize()
        {
            writer.Dispose();
        }
    }
}