// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Drawing;

namespace FasterPSFSample
{
    public struct BlittableOrders : IOrders
    {
        // Colors, strings, and enums are not blittable so we use int
        public int Size { get; set; }

        public int Color { get; set; }

        public int NumSold { get; set; }

        public BlittableOrders(Constants.Size size, Color color, int numSold)
        {
            this.Size = (int)size;
            this.Color = color.ToArgb();
            this.NumSold = numSold;
        }

        public (int, int, int) MemberTuple => (this.Size, this.Color, this.NumSold);

        public override string ToString() => $"{(Constants.Size)this.Size}, {Constants.ColorDict[this.Color].Name}, {NumSold}";

        public class Functions : IFunctions<Key, BlittableOrders, Input, Output<BlittableOrders>, Context<BlittableOrders>>
        {
            public void ConcurrentReader(ref Key key, ref Input input, ref BlittableOrders value, ref Output<BlittableOrders> dst)
                => dst.Value = value;

            public bool ConcurrentWriter(ref Key key, ref BlittableOrders src, ref BlittableOrders dst)
            {
                dst = src;
                return true;
            }

            public void CopyUpdater(ref Key key, ref Input input, ref BlittableOrders oldValue, ref BlittableOrders newValue)
                => throw new NotImplementedException();

            public void InitialUpdater(ref Key key, ref Input input, ref BlittableOrders value)
                => throw new NotImplementedException();

            public bool InPlaceUpdater(ref Key key, ref Input input, ref BlittableOrders value)
                => throw new NotImplementedException();

            public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
                => throw new NotImplementedException();

            public void ReadCompletionCallback(ref Key key, ref Input input, ref Output<BlittableOrders> output, Context<BlittableOrders> context, Status status)
            {
                if (output.Value.MemberTuple != key.MemberTuple)
                    throw new Exception("Read mismatch error!");
            }

            public void RMWCompletionCallback(ref Key key, ref Input input, Context<BlittableOrders> context, Status status)
                => throw new NotImplementedException();

            public void SingleReader(ref Key key, ref Input input, ref BlittableOrders value, ref Output<BlittableOrders> dst)
                => dst.Value = value;

            public void SingleWriter(ref Key key, ref BlittableOrders src, ref BlittableOrders dst)
                => dst = src;

            public void UpsertCompletionCallback(ref Key key, ref BlittableOrders value, Context<BlittableOrders> context)
                => throw new NotImplementedException();

            public void DeleteCompletionCallback(ref Key key, Context<BlittableOrders> context)
                => throw new NotImplementedException();
        }
    }
}
