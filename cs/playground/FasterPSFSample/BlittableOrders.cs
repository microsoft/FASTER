// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace FasterPSFSample
{
    public struct BlittableOrders : IOrders
    {
        public int Id { get; set; }

        // Colors, strings, and enums are not blittable so we use int
        public int SizeInt { get; set; }

        public int ColorArgb { get; set; }

        public int Count { get; set; }

        public override string ToString() => $"{(Constants.Size)this.SizeInt}, {Constants.ColorDict[this.ColorArgb].Name}, {Count}";

        public class Functions : IFunctions<Key, BlittableOrders, Input<BlittableOrders>, Output<BlittableOrders>, Context<BlittableOrders>>
        {
            #region Read
            public void ConcurrentReader(ref Key key, ref Input<BlittableOrders> input, ref BlittableOrders value, ref Output<BlittableOrders> dst)
                => dst.Value = value;

            public void SingleReader(ref Key key, ref Input<BlittableOrders> input, ref BlittableOrders value, ref Output<BlittableOrders> dst)
                => dst.Value = value;

            public void ReadCompletionCallback(ref Key key, ref Input<BlittableOrders> input, ref Output<BlittableOrders> output, Context<BlittableOrders> context, Status status)
            { /* Output is not set by pending operations */ }
            #endregion Read

            #region Upsert
            public bool ConcurrentWriter(ref Key key, ref BlittableOrders src, ref BlittableOrders dst)
            {
                dst = src;
                return true;
            }

            public void SingleWriter(ref Key key, ref BlittableOrders src, ref BlittableOrders dst)
                => dst = src;

            public void UpsertCompletionCallback(ref Key key, ref BlittableOrders value, Context<BlittableOrders> context)
            { }
            #endregion Upsert

            #region RMW
            public void CopyUpdater(ref Key key, ref Input<BlittableOrders> input, ref BlittableOrders oldValue, ref BlittableOrders newValue)
                => throw new NotImplementedException();

            public void InitialUpdater(ref Key key, ref Input<BlittableOrders> input, ref BlittableOrders value)
                => value = input.InitialUpdateValue;

            public bool InPlaceUpdater(ref Key key, ref Input<BlittableOrders> input, ref BlittableOrders value)
            {
                value.ColorArgb = input.IPUColorInt;
                return true;
            }

            public void RMWCompletionCallback(ref Key key, ref Input<BlittableOrders> input, Context<BlittableOrders> context, Status status)
            { }
            #endregion RMW

            public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
            { }

            public void DeleteCompletionCallback(ref Key key, Context<BlittableOrders> context)
            { }
        }
    }
}
