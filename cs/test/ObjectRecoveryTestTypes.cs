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
    public class AdId : IKey<AdId>
    {
        public long adId;

        public long GetHashCode64()
        {
            return Utility.GetHashCode(adId);
        }
        public bool Equals(ref AdId k2)
        {
            return adId == k2.adId;
        }
    }

    public class AdIdSerializer : BinaryObjectSerializer<AdId>
    {
        public override void Deserialize(ref AdId obj)
        {
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
        public override void Deserialize(ref NumClicks obj)
        {
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

        public void PersistenceCallback(long thread_id, long serial_num)
        {
            Console.WriteLine("Thread {0} reports persistence until {1}", thread_id, serial_num);
        }

        // Read functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleReader(ref AdId key, ref Input input, ref NumClicks value, ref Output dst)
        {
            dst.value = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ConcurrentReader(ref AdId key, ref Input input, ref NumClicks value, ref Output dst)
        {
            dst.value = value;
        }

        // Upsert functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleWriter(ref AdId key, ref NumClicks src, ref NumClicks dst)
        {
            dst = src;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ConcurrentWriter(ref AdId key, ref NumClicks src, ref NumClicks dst)
        {
            dst = src;
        }

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int InitialValueLength(ref AdId key, ref Input input)
        {
            return 8;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitialUpdater(ref AdId key, ref Input input, ref NumClicks value)
        {
            value = input.numClicks;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InPlaceUpdater(ref AdId key, ref Input input, ref NumClicks value)
        {
            Interlocked.Add(ref value.numClicks, input.numClicks.numClicks);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyUpdater(ref AdId key, ref Input input, ref NumClicks oldValue, ref NumClicks newValue)
        {
            newValue = new NumClicks { numClicks = oldValue.numClicks + input.numClicks.numClicks };
        }
    }
}
