using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FASTER.core;
using Microsoft.StreamProcessing.Serializer;
using Microsoft.StreamProcessing.Sharding;

namespace FASTER.serializers
{
    class StreamSerializer<T> : IObjectSerializer<T>
    {
        static StateSerializer<T> streamSerializer
            = StreamableSerializer.Create<T>();
        private Stream stream;

        public void BeginDeserialize(Stream stream)
        {
            this.stream = stream;
        }

        public void BeginSerialize(Stream stream)
        {
            this.stream = stream;
        }

        public void Deserialize(ref T obj)
        {
            obj = streamSerializer.Deserialize(stream);
        }

        public void EndDeserialize()
        {
        }

        public void EndSerialize()
        {
        }

        public void Serialize(ref T obj)
        {
            streamSerializer.Serialize(stream, obj);
        }
    }
}
