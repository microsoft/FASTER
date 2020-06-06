// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace FasterPSFSample
{
    public class ObjectOrders : IOrders
    {
        public int Id { get; set; }

        // Colors, strings, and enums are not blittable so we use int
        public int SizeInt { get => values[0]; set => values[0] = value;  }

        public int ColorArgb { get => values[1]; set => values[1] = value; }

        public int NumSold { get => values[2]; set => values[2] = value; }

        public int[] values;

        public ObjectOrders() => throw new InvalidOperationException("Must use ctor overload");

        public override string ToString() => $"{(Constants.Size)this.SizeInt}, {Constants.ColorDict[this.ColorArgb].Name}, {NumSold}";

        public class Serializer : BinaryObjectSerializer<ObjectOrders>
        {
            public override void Deserialize(ref ObjectOrders obj)
            {
                obj.values = new int[3];
                for (var ii = 0; ii < obj.values.Length; ++ii)
                    obj.values[ii] = reader.ReadInt32();
            }

            public override void Serialize(ref ObjectOrders obj)
            {
                for (var ii = 0; ii < obj.values.Length; ++ii)
                    writer.Write(obj.values[ii]);
            }
        }

        public class Functions : IFunctions<Key, ObjectOrders, Input, Output<ObjectOrders>, Context<ObjectOrders>>
        {
            #region Read
            public void ConcurrentReader(ref Key key, ref Input input, ref ObjectOrders value, ref Output<ObjectOrders> dst)
                => dst.Value = value;

            public void SingleReader(ref Key key, ref Input input, ref ObjectOrders value, ref Output<ObjectOrders> dst)
                => dst.Value = value;

            public void ReadCompletionCallback(ref Key key, ref Input input, ref Output<ObjectOrders> output, Context<ObjectOrders> context, Status status)
            {
                if (((IOrders)output.Value).MemberTuple != key.MemberTuple)
                    throw new Exception("Read mismatch error!");
            }
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
                => throw new NotImplementedException();
            #endregion Upsert

            #region RMW
            public void CopyUpdater(ref Key key, ref Input input, ref ObjectOrders oldValue, ref ObjectOrders newValue)
                => throw new NotImplementedException();

            public void InitialUpdater(ref Key key, ref Input input, ref ObjectOrders value)
                => throw new NotImplementedException();

            public bool InPlaceUpdater(ref Key key, ref Input input, ref ObjectOrders value)
            {
                value.ColorArgb = FasterPSFSample.IPUColor;
                return true;
            }

            public void RMWCompletionCallback(ref Key key, ref Input input, Context<ObjectOrders> context, Status status)
                => throw new NotImplementedException();
            #endregion RMW

            public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
                => throw new NotImplementedException();

            public void DeleteCompletionCallback(ref Key key, Context<ObjectOrders> context)
                => throw new NotImplementedException();
        }
    }
}
