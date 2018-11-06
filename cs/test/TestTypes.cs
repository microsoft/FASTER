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

namespace FASTER.test
{

    public unsafe struct KeyStruct
    {
        public const int physicalSize = sizeof(long) + sizeof(long);
        public long kfield1;
        public long kfield2;

        public static long GetHashCode(KeyStruct* key)
        {
            return Utility.GetHashCode(*((long*)key));
        }
        public static bool Equals(KeyStruct* k1, KeyStruct* k2)
        {
            return k1->kfield1 == k2->kfield1 && k1->kfield2 == k2->kfield2;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength(KeyStruct* key)
        {
            return physicalSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(KeyStruct* src, KeyStruct* dst)
        {
            dst->kfield1 = src->kfield1;
            dst->kfield2 = src->kfield2;
        }

        #region Serialization
        public static bool HasObjectsToSerialize()
        {
            return false;
        }

        public static void Serialize(KeyStruct* key, Stream toStream)
        {
            throw new InvalidOperationException();
        }

        public static void Deserialize(KeyStruct* key, Stream fromStream)
        {
            throw new InvalidOperationException();
        }
        public static void Free(KeyStruct* key)
        {
            throw new InvalidOperationException();
        }
        #endregion

        public static KeyStruct* MoveToContext(KeyStruct* key)
        {
            return key;
        }
    }
    public unsafe struct ValueStruct
    {
        public const int physicalSize = sizeof(long) + sizeof(long);
        public long vfield1;
        public long vfield2;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength(ValueStruct* input)
        {
            return physicalSize;
        }

        public static void Copy(ValueStruct* src, ValueStruct* dst)
        {
            dst->vfield1 = src->vfield1;
            dst->vfield2 = src->vfield2;
        }

        // Shared read/write capabilities on value
        public static void AcquireReadLock(ValueStruct* value)
        {
        }

        public static void ReleaseReadLock(ValueStruct* value)
        {
        }

        public static void AcquireWriteLock(ValueStruct* value)
        {
        }

        public static void ReleaseWriteLock(ValueStruct* value)
        {
        }

        #region Serialization
        public static bool HasObjectsToSerialize()
        {
            return false;
        }

        public static void Serialize(ValueStruct* key, Stream toStream)
        {
            throw new InvalidOperationException();
        }

        public static void Deserialize(ValueStruct* key, Stream fromStream)
        {
            throw new InvalidOperationException();
        }
        public static void Free(ValueStruct* key)
        {
            throw new InvalidOperationException();
        }
        #endregion

        public static ValueStruct* MoveToContext(ValueStruct* value)
        {
            return value;
        }
    }
    public unsafe struct InputStruct
    {
        public long ifield1;
        public long ifield2;

        public static InputStruct* MoveToContext(InputStruct* input)
        {
            return input;
        }
    }
    public unsafe struct OutputStruct
    {
        public ValueStruct value;

        public static OutputStruct* MoveToContext(OutputStruct* output)
        {
            return output;
        }

    }
    public unsafe interface ICustomFaster
    {
        /* Thread-related operations */
        Guid StartSession();
        long ContinueSession(Guid guid);
        void StopSession();
        void Refresh();

        /* Store Interface */
        Status Read(KeyStruct* key, InputStruct* input, OutputStruct* output, Empty* context, long lsn);
        Status Upsert(KeyStruct* key, ValueStruct* value, Empty* context, long lsn);
        Status RMW(KeyStruct* key, InputStruct* input, Empty* context, long lsn);
        bool CompletePending(bool wait);
        bool ShiftBeginAddress(long untilAddress);

        /* Statistics */
        long LogTailAddress { get; }
        long LogReadOnlyAddress { get; }

        void DumpDistribution();
    }
    public unsafe class Functions
    {
        public static void RMWCompletionCallback(KeyStruct* key, InputStruct* output, Empty* ctx, Status status)
        {
        }

        public static void ReadCompletionCallback(KeyStruct* key, InputStruct* input, OutputStruct* output, Empty* ctx, Status status)
        {
        }

        public static void UpsertCompletionCallback(KeyStruct* key, ValueStruct* output, Empty* ctx)
        {
        }

        public static void PersistenceCallback(long thread_id, long serial_num)
        {
            Debug.WriteLine("Thread {0} reports persistence until {1}", thread_id, serial_num);
        }

        // Read functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SingleReader(KeyStruct* key, InputStruct* input, ValueStruct* value, OutputStruct* dst)
        {
            ValueStruct.Copy(value, (ValueStruct*)dst);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ConcurrentReader(KeyStruct* key, InputStruct* input, ValueStruct* value, OutputStruct* dst)
        {
            ValueStruct.AcquireReadLock(value);
            ValueStruct.Copy(value, (ValueStruct*)dst);
            ValueStruct.ReleaseReadLock(value);
        }

        // Upsert functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SingleWriter(KeyStruct* key, ValueStruct* src, ValueStruct* dst)
        {
            ValueStruct.Copy(src, dst);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ConcurrentWriter(KeyStruct* key, ValueStruct* src, ValueStruct* dst)
        {
            ValueStruct.AcquireWriteLock(dst);
            ValueStruct.Copy(src, dst);
            ValueStruct.ReleaseWriteLock(dst);
        }

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int InitialValueLength(KeyStruct* key, InputStruct* input)
        {
            return ValueStruct.GetLength(default(ValueStruct*));
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void InitialUpdater(KeyStruct* key, InputStruct* input, ValueStruct* value)
        {
            ValueStruct.Copy((ValueStruct*)input, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void InPlaceUpdater(KeyStruct* key, InputStruct* input, ValueStruct* value)
        {
            ValueStruct.AcquireWriteLock(value);
            value->vfield1 += input->ifield1;
            value->vfield2 += input->ifield2;
            ValueStruct.ReleaseWriteLock(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CopyUpdater(KeyStruct* key, InputStruct* input, ValueStruct* oldValue, ValueStruct* newValue)
        {
            newValue->vfield1 = oldValue->vfield1 + input->ifield1;
            newValue->vfield2 = oldValue->vfield2 + input->ifield2;
        }
    }

}
