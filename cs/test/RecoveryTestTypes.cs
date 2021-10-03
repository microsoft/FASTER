// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Threading;
using FASTER.core;

namespace FASTER.test.recovery.sumstore
{
    public struct AdId : IFasterEqualityComparer<AdId>
    {
        public long adId;

        public long GetHashCode64(ref AdId key) => Utility.GetHashCode(key.adId);

        public bool Equals(ref AdId k1, ref AdId k2) => k1.adId == k2.adId;

        public override string ToString() => adId.ToString();
    }

    public struct AdInput
    {
        public AdId adId;
        public NumClicks numClicks;

        public override string ToString() => $"id = {adId.adId}, clicks = {numClicks.numClicks}";
    }

    public struct NumClicks
    {
        public long numClicks;

        public override string ToString() => numClicks.ToString();
    }

    public struct Output
    {
        public NumClicks value;

        public override string ToString() => value.ToString();
    }

    public class Functions : FunctionsBase<AdId, NumClicks, AdInput, Output, Empty>
    {
        // Read functions
        public override void SingleReader(ref AdId key, ref AdInput input, ref NumClicks value, ref Output dst)
        {
            dst.value = value;
        }

        public override void ConcurrentReader(ref AdId key, ref AdInput input, ref NumClicks value, ref Output dst)
        {
            dst.value = value;
        }

        // RMW functions
        public override void InitialUpdater(ref AdId key, ref AdInput input, ref NumClicks value, ref Output output)
        {
            value = input.numClicks;
        }

        public override bool InPlaceUpdater(ref AdId key, ref AdInput input, ref NumClicks value, ref Output output)
        {
            Interlocked.Add(ref value.numClicks, input.numClicks.numClicks);
            return true;
        }

        public override bool NeedCopyUpdate(ref AdId key, ref AdInput input, ref NumClicks oldValue, ref Output output) => true;

        public override void CopyUpdater(ref AdId key, ref AdInput input, ref NumClicks oldValue, ref NumClicks newValue, ref Output output)
        {
            newValue.numClicks += oldValue.numClicks + input.numClicks.numClicks;
        }
    }
}
