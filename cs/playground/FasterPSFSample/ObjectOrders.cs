// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace FasterPSFSample
{
    public class ObjectOrders : IOrders
    {
        const int numValues = 3;
        const int sizeOrd = 0;
        const int colorOrd = 1;
        const int countOrd = 2;

        public int Id { get; set; }

        // Colors, strings, and enums are not blittable so we use int
        public int SizeInt { get => values[sizeOrd]; set => values[sizeOrd] = value;  }

        public int ColorArgb { get => values[colorOrd]; set => values[colorOrd] = value; }

        public int Count { get => values[countOrd]; set => values[countOrd] = value; }

        public int[] values = new int[numValues];

        public override string ToString() => $"{(Constants.Size)this.SizeInt}, {Constants.ColorDict[this.ColorArgb].Name}, {Count}";

        public class Serializer : BinaryObjectSerializer<ObjectOrders>
        {
            public override void Deserialize(ref ObjectOrders obj)
            {
                obj.values = new int[numValues];
                for (var ii = 0; ii < obj.values.Length; ++ii)
                    obj.values[ii] = reader.ReadInt32();
            }

            public override void Serialize(ref ObjectOrders obj)
            {
                for (var ii = 0; ii < obj.values.Length; ++ii)
                    writer.Write(obj.values[ii]);
            }
        }

        public class Functions : IFunctions<Key, ObjectOrders, Input<ObjectOrders>, Output<ObjectOrders>, Context<ObjectOrders>>
        {
            public ObjectOrders InitialUpdateValue { get; set; }

            #region Read
            public void ConcurrentReader(ref Key key, ref Input<ObjectOrders> input, ref ObjectOrders value, ref Output<ObjectOrders> dst)
                => dst.Value = value;

            public void SingleReader(ref Key key, ref Input<ObjectOrders> input, ref ObjectOrders value, ref Output<ObjectOrders> dst)
                => dst.Value = value;

            public void ReadCompletionCallback(ref Key key, ref Input<ObjectOrders> input, ref Output<ObjectOrders> output, Context<ObjectOrders> context, Status status)
            { /* Output is not set by pending operations */ }
            #endregion Read

            #region Upsert
            public bool ConcurrentWriter(ref Key key, ref ObjectOrders src, ref ObjectOrders dst)
            {
                dst = src;
                return true;
            }

            public void SingleWriter(ref Key key, ref ObjectOrders src, ref ObjectOrders dst)
                => dst = src;

            public void UpsertCompletionCallback(ref Key key, ref ObjectOrders value, Context<ObjectOrders> context)
            { }
            #endregion Upsert

            #region RMW
            public void CopyUpdater(ref Key key, ref Input<ObjectOrders> input, ref ObjectOrders oldValue, ref ObjectOrders newValue)
                => throw new NotImplementedException();

            public void InitialUpdater(ref Key key, ref Input<ObjectOrders> input, ref ObjectOrders value)
                => value = input.InitialUpdateValue;

            public bool InPlaceUpdater(ref Key key, ref Input<ObjectOrders> input, ref ObjectOrders value)
            {
                value.ColorArgb = input.IPUColorInt;
                return true;
            }

            public void RMWCompletionCallback(ref Key key, ref Input<ObjectOrders> input, Context<ObjectOrders> context, Status status)
            { }
            #endregion RMW

            public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
            { }

            public void DeleteCompletionCallback(ref Key key, Context<ObjectOrders> context)
            { }
        }
    }
}
