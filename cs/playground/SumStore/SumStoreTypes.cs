// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;
using FASTER.core;

namespace SumStore
{
    public struct AdId : IFasterEqualityComparer<AdId>
    {
        public long adId;

        public long GetHashCode64(ref AdId key)
        {
            return Utility.GetHashCode(key.adId);
        }
        public bool Equals(ref AdId k1, ref AdId k2)
        {
            return k1.adId == k2.adId;
        }
    }

    public struct Input
    {
        public AdId adId;
        public NumClicks numClicks;
    }

    public struct NumClicks
    {
        public long numClicks;
    }

    public struct Output
    {
        public NumClicks value;
    }

    public sealed class Functions : FunctionsBase<AdId, NumClicks, Input, Output, Empty>
    {
        public override void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
        {
            Console.WriteLine("Session {0} reports persistence until {1}", sessionId, commitPoint.UntilSerialNo);
        }

        // Read functions
        public override void SingleReader(ref AdId key, ref Input input, ref NumClicks value, ref Output dst)
        {
            dst.value = value;
        }

        public override void ConcurrentReader(ref AdId key, ref Input input, ref NumClicks value, ref Output dst)
        {
            dst.value = value;
        }

        // RMW functions
        public override void InitialUpdater(ref AdId key, ref Input input, ref NumClicks value, ref Output output)
        {
            value = input.numClicks;
        }

        public override bool InPlaceUpdater(ref AdId key, ref Input input, ref NumClicks value, ref Output output)
        {
            Interlocked.Add(ref value.numClicks, input.numClicks.numClicks);
            return true;
        }

        public override void CopyUpdater(ref AdId key, ref Input input, ref NumClicks oldValue, ref NumClicks newValue, ref Output output)
        {
            newValue.numClicks = oldValue.numClicks + input.numClicks.numClicks;
        }
    }
}
