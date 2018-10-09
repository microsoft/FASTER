// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using FASTER.core;
using System.Runtime.CompilerServices;
using System.IO;
using System.Diagnostics;

namespace FASTER.test.recovery.objectstore
{
    public class AdId : IFasterKey<AdId>
    {
        public long adId;

        public AdId Clone()
        {
            return this;
        }

        public long GetHashCode64()
        {
            return Utility.GetHashCode(adId);
        }

        public bool Equals(AdId otherAdId)
        {
            return adId == otherAdId.adId;
        }
        public void Serialize(Stream toStream)
        {
            new BinaryWriter(toStream).Write(adId);
        }

        public void Deserialize(Stream fromStream)
        {
            adId = new BinaryReader(fromStream).ReadInt64();
        }
    }


    public class NumClicks : IFasterValue<NumClicks>
    {
        public long numClicks;

        public NumClicks Clone()
        {
            return this;
        }
        public void Serialize(Stream toStream)
        {
            new BinaryWriter(toStream).Write(numClicks);
        }

        public void Deserialize(Stream fromStream)
        {
            numClicks = new BinaryReader(fromStream).ReadInt64();
        }
    }


    public class Input
    {
        public long numClicks;
    }

    public class Output
    {
        public NumClicks numClicks;
    }


    public class Functions : IUserFunctions<AdId, NumClicks, Input, Output, Empty>
    {
        public void CopyUpdater(AdId key, Input input, NumClicks oldValue, ref NumClicks newValue)
        {
            newValue = new NumClicks { numClicks = oldValue.numClicks + input.numClicks };
        }

        public void InitialUpdater(AdId key, Input input, ref NumClicks value)
        {
            value = new NumClicks { numClicks = input.numClicks };
        }

        public void InPlaceUpdater(AdId key, Input input, ref NumClicks value)
        {
            value.numClicks += input.numClicks;
        }

        public void ReadCompletionCallback(Empty ctx, Output output, Status status)
        {
        }

        public void Reader(AdId key, Input input, NumClicks value, ref Output dst)
        {
            dst.numClicks = value;
        }

        public void RMWCompletionCallback(Empty ctx, Status status)
        {
        }

        public void UpsertCompletionCallback(Empty ctx)
        {
        }
    }
}
