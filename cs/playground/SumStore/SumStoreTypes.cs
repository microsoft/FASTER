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

    public class Functions : IFunctions<AdId, NumClicks, Input, Output, Empty>
    {
        public void RMWCompletionCallback(ref AdId key, ref Input input, Empty ctx, Status status)
        {
        }

        public void ReadCompletionCallback(ref AdId key, ref Input input, ref Output output, Empty ctx, Status status)
        {
        }

        public void UpsertCompletionCallback(ref AdId key, ref NumClicks input, Empty ctx)
        {
        }

        public void DeleteCompletionCallback(ref AdId key, Empty ctx)
        {
        }

        public void CheckpointCompletionCallback(Guid sessionId, long serialNum)
        {
            Console.WriteLine("Session {0} reports persistence until {1}", sessionId, serialNum);
        }

        // Read functions
        public void SingleReader(ref AdId key, ref Input input, ref NumClicks value, ref Output dst)
        {
            dst.value = value;
        }

        public void ConcurrentReader(ref AdId key, ref Input input, ref NumClicks value, ref Output dst)
        {
            dst.value = value;
        }

        // Upsert functions
        public void SingleWriter(ref AdId key, ref NumClicks src, ref NumClicks dst)
        {
            dst = src;
        }

        public bool ConcurrentWriter(ref AdId key, ref NumClicks src, ref NumClicks dst)
        {
            dst = src;
            return true;
        }

        // RMW functions
        public void InitialUpdater(ref AdId key, ref Input input, ref NumClicks value)
        {
            value = input.numClicks;
        }

        public bool InPlaceUpdater(ref AdId key, ref Input input, ref NumClicks value)
        {
            Interlocked.Add(ref value.numClicks, input.numClicks.numClicks);
            return true;
        }

        public void CopyUpdater(ref AdId key, ref Input input, ref NumClicks oldValue, ref NumClicks newValue)
        {
            newValue.numClicks = oldValue.numClicks + input.numClicks.numClicks;
        }
    }
}
