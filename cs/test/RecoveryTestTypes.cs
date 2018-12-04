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

namespace FASTER.test.recovery.sumstore
{
    public unsafe struct AdId : IKey<AdId>
    {
        public const int physicalSize = sizeof(long);
        public long adId;

        public long GetHashCode64()
        {
            return Utility.GetHashCode(adId);
        }
        public bool Equals(ref AdId k2)
        {
            return adId == k2.adId;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetLength()
        {
            return physicalSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ShallowCopy(ref AdId dst)
        {
            dst.adId = adId;
        }

        public ref AdId MoveToContext(ref AdId value)
        {
            return ref value;
        }
        #region Serialization
        public bool HasObjectsToSerialize()
        {
            return false;
        }

        public void Serialize(Stream toStream)
        {
            throw new InvalidOperationException();
        }

        public void Deserialize(Stream fromStream)
        {
            throw new InvalidOperationException();
        }
        public void Free()
        {
            throw new InvalidOperationException();
        }
        #endregion
    }

    public unsafe struct Input : IMoveToContext<Input>
    {
        public AdId adId;
        public NumClicks numClicks;

        public ref Input MoveToContext(ref Input value)
        {
            return ref value;
        }

    }

    public unsafe struct NumClicks : IValue<NumClicks>
    {
        public const int physicalSize = sizeof(long);
        public long numClicks;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetLength()
        {
            return physicalSize;
        }

        public void ShallowCopy(ref NumClicks dst)
        {
            dst.numClicks = numClicks;
        }

        // Shared read/write capabilities on value
        public void AcquireReadLock()
        {
        }

        public void ReleaseReadLock()
        {
        }

        public void AcquireWriteLock()
        {
        }

        public void ReleaseWriteLock()
        {
        }

        public ref NumClicks MoveToContext(ref NumClicks value)
        {
            return ref value;
        }

        #region Serialization
        public bool HasObjectsToSerialize()
        {
            return false;
        }

        public void Serialize(Stream toStream)
        {
            throw new InvalidOperationException();
        }

        public void Deserialize(Stream fromStream)
        {
            throw new InvalidOperationException();
        }
        public void Free()
        {
            throw new InvalidOperationException();
        }
        #endregion
    }

    public unsafe struct Output : IMoveToContext<Output>
    {
        public NumClicks value;

        public ref Output MoveToContext(ref Output value)
        {
            return ref value;
        }

    }

    public unsafe class Functions : IFunctions<AdId, NumClicks, Input, Output, Empty>
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
            value.AcquireReadLock();
            dst.value = value;
            value.ReleaseReadLock();
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
            dst.AcquireWriteLock();
            dst = src;
            dst.ReleaseWriteLock();
        }

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int InitialValueLength(ref AdId key, ref Input input)
        {
            return default(NumClicks).GetLength();
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
            newValue.numClicks += oldValue.numClicks + input.numClicks.numClicks;
        }
    }

    public unsafe interface ICustomFaster
    {
        /* Thread-related operations */
        Guid StartSession();
        long ContinueSession(Guid guid);
        void StopSession();
        void Refresh();
        bool TakeFullCheckpoint(out Guid token);
        bool TakeIndexCheckpoint(out Guid token);
        bool TakeHybridLogCheckpoint(out Guid token);
        void Recover(Guid fullcheckpointToken);
        void Recover(Guid indexToken, Guid hybridLogToken);
        bool CompleteCheckpoint(bool wait);

        /* Store Interface */
        Status Read(AdId* key, Input* input, Output* output, Empty* context, long lsn);
        Status Upsert(AdId* key, NumClicks* value, Empty* context, long lsn);
        Status RMW(AdId* key, Input* input, Empty* context, long lsn);
        bool CompletePending(bool wait);
        bool ShiftBeginAddress(long untilAddress);

        /* Statistics */
        long LogTailAddress { get; }
        long LogReadOnlyAddress { get; }
        void DumpDistribution();
        void Dispose();
    }
}
