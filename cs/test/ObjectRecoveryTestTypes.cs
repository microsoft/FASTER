// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

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

    public class Functions : IFunctions<AdId, NumClicks, Input, Output, Empty>
    {
        public void RMWCompletionCallback(ref AdId key, ref Input input, ref Empty ctx, Status status)
        {
        }

        public void ReadCompletionCallback(ref AdId key, ref Input input, ref Output output, ref Empty ctx, Status status)
        {
        }

        public void UpsertCompletionCallback(ref AdId key, ref NumClicks input, ref Empty ctx)
        {
        }

        public void DeleteCompletionCallback(ref AdId key, ref Empty ctx)
        {
        }

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
        {
            Console.WriteLine("Session {0} reports persistence until {1}", sessionId, commitPoint.UntilSerialNo);
        }

        // Read functions
        public void SingleReader(ref AdId key, ref Input input, ref NumClicks value, ref Output dst, ref Empty ctx)
        {
            dst.value = value;
        }

        public void ConcurrentReader(ref AdId key, ref Input input, ref NumClicks value, ref Output dst, ref Empty ctx)
        {
            dst.value = value;
        }

        // Upsert functions
        public void SingleWriter(ref AdId key, ref NumClicks src, ref NumClicks dst, ref Empty ctx)
        {
            dst = src;
        }

        public bool ConcurrentWriter(ref AdId key, ref NumClicks src, ref NumClicks dst, ref Empty ctx)
        {
            dst = src;
            return true;
        }

        // RMW functions
        public void InitialUpdater(ref AdId key, ref Input input, ref NumClicks value, ref Empty ctx)
        {
            value = input.numClicks;
        }

        public bool InPlaceUpdater(ref AdId key, ref Input input, ref NumClicks value, ref Empty ctx)
        {
            Interlocked.Add(ref value.numClicks, input.numClicks.numClicks);
            return true;
        }

        public void CopyUpdater(ref AdId key, ref Input input, ref NumClicks oldValue, ref NumClicks newValue, ref Empty ctx)
        {
            newValue = new NumClicks { numClicks = oldValue.numClicks + input.numClicks.numClicks };
        }
    }
}
