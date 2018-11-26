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
    public struct KeyStruct : IKey<KeyStruct>
    {
        public const int physicalSize = sizeof(long) + sizeof(long);
        public long kfield1;
        public long kfield2;

        public long GetHashCode64()
        {
            return Utility.GetHashCode(kfield1);
        }
        public bool Equals(ref KeyStruct k2)
        {
            return kfield1 == k2.kfield1 && kfield2 == k2.kfield2;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetLength()
        {
            return physicalSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ShallowCopy(ref KeyStruct dst)
        {
            dst.kfield1 = kfield1;
            dst.kfield2 = kfield2;
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

        public ref KeyStruct MoveToContext(ref KeyStruct key)
        {
            return ref key;
        }
    }

    public struct ValueStruct : IValue<ValueStruct>
    {
        public const int physicalSize = sizeof(long) + sizeof(long);
        public long vfield1;
        public long vfield2;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetLength()
        {
            return physicalSize;
        }

        public void ShallowCopy(ref ValueStruct dst)
        {
            dst.vfield1 = vfield1;
            dst.vfield2 = vfield2;
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

        public ref ValueStruct MoveToContext(ref ValueStruct value)
        {
            return ref value;
        }
    }

    public struct InputStruct : IMoveToContext<InputStruct>
    {
        public long ifield1;
        public long ifield2;

        public ref InputStruct MoveToContext(ref InputStruct input)
        {
            return ref input;
        }
    }

    public struct OutputStruct : IMoveToContext<OutputStruct>
    {
        public ValueStruct value;

        public ref OutputStruct MoveToContext(ref OutputStruct output)
        {
            return ref output;
        }
    }

    public class Functions : IFunctions<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty>
    {
        public void RMWCompletionCallback(ref KeyStruct key, ref InputStruct output, ref Empty ctx, Status status)
        {
        }

        public void ReadCompletionCallback(ref KeyStruct key, ref InputStruct input, ref OutputStruct output, ref Empty ctx, Status status)
        {
        }

        public void UpsertCompletionCallback(ref KeyStruct key, ref ValueStruct output, ref Empty ctx)
        {
        }

        public void PersistenceCallback(long thread_id, long serial_num)
        {
            Debug.WriteLine("Thread {0} repors persistence until {1}", thread_id, serial_num);
        }

        // Read functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst)
        {
            dst.value = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ConcurrentReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst)
        {
            value.AcquireReadLock();
            dst.value = value;
            value.ReleaseReadLock();
        }

        // Upsert functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SingleWriter(ref KeyStruct key, ref ValueStruct src, ref ValueStruct dst)
        {
            dst = src;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ConcurrentWriter(ref KeyStruct key, ref ValueStruct src, ref ValueStruct dst)
        {
            dst.AcquireWriteLock();
            dst = src;
            dst.ReleaseWriteLock();
        }

        // RMW functions
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int InitialValueLength(ref KeyStruct key, ref InputStruct input)
        {
            return default(ValueStruct).GetLength();
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitialUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct value)
        {
            value.vfield1 = input.ifield1;
            value.vfield2 = input.ifield2;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InPlaceUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct value)
        {
            value.AcquireWriteLock();
            value.vfield1 += input.ifield1;
            value.vfield2 += input.ifield2;
            value.ReleaseWriteLock();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyUpdater(ref KeyStruct key, ref InputStruct input, ref ValueStruct oldValue, ref ValueStruct newValue)
        {
            newValue.vfield1 = oldValue.vfield1 + input.ifield1;
            newValue.vfield2 = oldValue.vfield2 + input.ifield2;
        }
    }
}
