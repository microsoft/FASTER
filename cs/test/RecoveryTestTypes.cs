// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FASTER.core;
using System.Runtime.CompilerServices;
using System.IO;
using System.Diagnostics;

namespace FASTER.test.recovery.sumstore
{
    public unsafe struct AdId
    {
        public const int physicalSize = sizeof(long);
        public long adId;

        public static long GetHashCode(AdId* key)
        {
            return Utility.GetHashCode(*((long*)key));
        }
        public static bool Equals(AdId* k1, AdId* k2)
        {
            return k1->adId == k2->adId;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength(AdId* key)
        {
            return physicalSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(AdId* src, AdId* dst)
        {
            dst->adId = src->adId;
        }

        public static AdId* MoveToContext(AdId* value)
        {
            return value;
        }
        #region Serialization
        public static bool HasObjectsToSerialize()
        {
            return false;
        }

        public static void Serialize(AdId* key, Stream toStream)
        {
            throw new InvalidOperationException();
        }

        public static void Deserialize(AdId* key, Stream fromStream)
        {
            throw new InvalidOperationException();
        }
        public static void Free(AdId* key)
        {
            throw new InvalidOperationException();
        }
        #endregion
    }

    public unsafe struct Input
    {
        public AdId adId;
        public NumClicks numClicks;

        public static Input* MoveToContext(Input* value)
        {
            return value;
        }

    }

    public unsafe struct NumClicks
    {
        public const int physicalSize = sizeof(long);
        public long numClicks;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength(NumClicks* input)
        {
            return physicalSize;
        }

        public static void Copy(NumClicks* src, NumClicks* dst)
        {
            dst->numClicks = src->numClicks;
        }

        // Shared read/write capabilities on value
        public static void AcquireReadLock(NumClicks* value)
        {
        }

        public static void ReleaseReadLock(NumClicks* value)
        {
        }

        public static void AcquireWriteLock(NumClicks* value)
        {
        }

        public static void ReleaseWriteLock(NumClicks* value)
        {
        }

        public static NumClicks* MoveToContext(NumClicks* value)
        {
            return value;
        }

        #region Serialization
        public static bool HasObjectsToSerialize()
        {
            return false;
        }

        public static void Serialize(NumClicks* key, Stream toStream)
        {
            throw new InvalidOperationException();
        }

        public static void Deserialize(NumClicks* key, Stream fromStream)
        {
            throw new InvalidOperationException();
        }
        public static void Free(NumClicks* key)
        {
            throw new InvalidOperationException();
        }
        #endregion
    }

    public unsafe struct Output
    {
        public NumClicks value;

        public static Output* MoveToContext(Output* value)
        {
            return value;
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

        /* Statistics */
        long LogTailAddress { get; }
        void DumpDistribution();
    }

    public unsafe class Functions
    {
        public static void RMWCompletionCallback(AdId* key, Input* input, Empty* ctx, Status status)
        {
        }

        public static void ReadCompletionCallback(AdId* key, Input* input, Output* output, Empty* ctx, Status status)
        {
        }

        public static void UpsertCompletionCallback(AdId* key, NumClicks* input, Empty* ctx)
        {
        }

        public static void PersistenceCallback(long thread_id, long serial_num)
        {
            Console.WriteLine("Thread {0} reports persistence until {1}", thread_id, serial_num);
        }

        // Read functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SingleReader(AdId* key, Input* input, NumClicks* value, Output* dst)
        {
            NumClicks.Copy(value, (NumClicks*)dst);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ConcurrentReader(AdId* key, Input* input, NumClicks* value, Output* dst)
        {
            NumClicks.AcquireReadLock(value);
            NumClicks.Copy(value, (NumClicks*)dst);
            NumClicks.ReleaseReadLock(value);
        }

        // Upsert functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SingleWriter(AdId* key, NumClicks* src, NumClicks* dst)
        {
            NumClicks.Copy(src, dst);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ConcurrentWriter(AdId* key, NumClicks* src, NumClicks* dst)
        {
            NumClicks.AcquireWriteLock(dst);
            NumClicks.Copy(src, dst);
            NumClicks.ReleaseWriteLock(dst);
        }

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int InitialValueLength(AdId* key, Input* input)
        {
            return NumClicks.GetLength(default(NumClicks*));
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void InitialUpdater(AdId* key, Input* input, NumClicks* value)
        {
            NumClicks.Copy(&input->numClicks, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void InPlaceUpdater(AdId* key, Input* input, NumClicks* value)
        {
            Interlocked.Add(ref value->numClicks, input->numClicks.numClicks);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CopyUpdater(AdId* key, Input* input, NumClicks* oldValue, NumClicks* newValue)
        {
            newValue->numClicks += oldValue->numClicks + input->numClicks.numClicks;
        }
    }
}
