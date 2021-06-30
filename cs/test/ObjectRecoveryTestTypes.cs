// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

using System.Threading;
using FASTER.core;

namespace FASTER.test.recovery.objectstore
{
    public class AdId : IFasterEqualityComparer<AdId>
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

    public class AdIdSerializer : BinaryObjectSerializer<AdId>
    {
        public override void Deserialize(out AdId obj)
        {
            obj = new AdId();
            obj.adId = reader.ReadInt64();
        }

        public override void Serialize(ref AdId obj)
        {
            writer.Write(obj.adId);
        }
    }


    public class Input
    {
        public AdId adId;
        public NumClicks numClicks;
    }

    public class NumClicks
    {
        public long numClicks;
    }

    public class NumClicksSerializer : BinaryObjectSerializer<NumClicks>
    {
        public override void Deserialize(out NumClicks obj)
        {
            obj = new NumClicks();
            obj.numClicks = reader.ReadInt64();
        }

        public override void Serialize(ref NumClicks obj)
        {
            writer.Write(obj.numClicks);
        }
    }


    public class Output
    {
        public NumClicks value;
    }

    public class Functions : FunctionsBase<AdId, NumClicks, Input, Output, Empty>
    {
        // Read functions
        public override void SingleReader(ref AdId key, ref Input input, ref NumClicks value, ref Output dst) => dst.value = value;

        public override void ConcurrentReader(ref AdId key, ref Input input, ref NumClicks value, ref Output dst) => dst.value = value;

        // RMW functions
        public override void InitialUpdater(ref AdId key, ref Input input, ref Output output, ref NumClicks value) => value = input.numClicks;

        public override bool InPlaceUpdater(ref AdId key, ref Input input, ref Output output, ref NumClicks value)
        {
            Interlocked.Add(ref value.numClicks, input.numClicks.numClicks);
            return true;
        }

        public override bool NeedCopyUpdate(ref AdId key, ref Input input, ref NumClicks oldValue) => true;

        public override void CopyUpdater(ref AdId key, ref Input input, ref Output output, ref NumClicks oldValue, ref NumClicks newValue)
        {
            newValue = new NumClicks { numClicks = oldValue.numClicks + input.numClicks.numClicks };
        }
    }
}
