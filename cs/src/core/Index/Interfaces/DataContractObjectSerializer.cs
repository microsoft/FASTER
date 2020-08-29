// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.IO;
using System.Runtime.Serialization;
using System.Xml;

namespace FASTER.core
{
    /// <summary>
    /// Serializer (for class types) based on DataContract
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class DataContractObjectSerializer<T> : BinaryObjectSerializer<T>
    {
        private static DataContractSerializer serializer = new DataContractSerializer(typeof(T));

        /// <summary>
        /// Deserialize
        /// </summary>
        /// <param name="obj"></param>
        public override void Deserialize(ref T obj)
        {
            int count = reader.ReadInt32();
            var byteArray = reader.ReadBytes(count);
            using var ms = new MemoryStream(byteArray);
            using (var _reader = XmlDictionaryReader.CreateBinaryReader(ms, XmlDictionaryReaderQuotas.Max))
                obj = (T)serializer.ReadObject(_reader);
        }

        /// <summary>
        /// Serialize
        /// </summary>
        /// <param name="obj"></param>
        public override void Serialize(ref T obj)
        {
            using var ms = new MemoryStream();
            using (var _writer = XmlDictionaryWriter.CreateBinaryWriter(ms, null, null, false))
                serializer.WriteObject(_writer, obj);
            writer.Write((int)ms.Position);
            writer.Write(ms.ToArray());
        }

        /// <summary>
        /// End serialize
        /// </summary>
        public void EndSerialize()
        {
            writer.Dispose();
        }
    }
}