// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Text;
using Newtonsoft.Json;

namespace Performance.Common
{
    internal static class JsonUtils
    {
        internal static JsonSerializerSettings OutputSerializerSettings = new JsonSerializerSettings { Formatting = Formatting.Indented };
        internal static JsonSerializerSettings InputSerializerSettings = new JsonSerializerSettings { ObjectCreationHandling = ObjectCreationHandling.Replace };

        internal static void WriteAllText(string filename, string jsonText)
        {
            var dir = Path.GetDirectoryName(filename);
            if (!string.IsNullOrEmpty(dir))
                Directory.CreateDirectory(dir);
            File.WriteAllText(filename, jsonText, Encoding.UTF8);
        }
    }

    public class DoubleRoundingConverter : JsonConverter
    {
        private const int Precision = 3;

        public DoubleRoundingConverter() { }

        public override bool CanRead => false;

        public override bool CanWrite => true;

        public override bool CanConvert(Type propertyType) => propertyType == typeof(double);

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            => throw new NotImplementedException();

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (value is double[] vector)
            {
                writer.WriteStartArray();
                foreach (var d in vector)
                    writer.WriteValue(Math.Round(d, Precision));
                writer.WriteEndArray();
                return;
            }
            writer.WriteValue(Math.Round((double)value, Precision));
        }
    }
}
