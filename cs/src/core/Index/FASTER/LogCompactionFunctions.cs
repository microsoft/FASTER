// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    internal sealed class LogVariableCompactFunctions<Key, Value, CompactionFunctions> : IFunctions<Key, Value, Empty, Empty, Empty>
        where CompactionFunctions : ICompactionFunctions<Key, Value>
    {
        private readonly VariableLengthBlittableAllocator<Key, Value> _allocator;
        private readonly CompactionFunctions _functions;

        public LogVariableCompactFunctions(VariableLengthBlittableAllocator<Key, Value> allocator, CompactionFunctions functions)
        {
            _allocator = allocator;
            _functions = functions;
        }

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }
        public void ConcurrentReader(ref Key key, ref Empty input, ref Value value, ref Empty dst) { }
        public bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst) { return _functions.CopyInPlace(ref src, ref dst, _allocator.ValueLength); }
        public void CopyUpdater(ref Key key, ref Empty input, ref Value oldValue, ref Value newValue) { }
        public void InitialUpdater(ref Key key, ref Empty input, ref Value value) { }
        public bool InPlaceUpdater(ref Key key, ref Empty input, ref Value value) => false;
        public void ReadCompletionCallback(ref Key key, ref Empty input, ref Empty output, Empty ctx, Status status) { }
        public void RMWCompletionCallback(ref Key key, ref Empty input, Empty ctx, Status status) { }
        public void SingleReader(ref Key key, ref Empty input, ref Value value, ref Empty dst) { }
        public void SingleWriter(ref Key key, ref Value src, ref Value dst) { _functions.Copy(ref src, ref dst, _allocator.ValueLength); }
        public void UpsertCompletionCallback(ref Key key, ref Value value, Empty ctx) { }
        public void DeleteCompletionCallback(ref Key key, Empty ctx) { }
    }

    internal sealed class LogCompactFunctions<Key, Value, CompactionFunctions> : IFunctions<Key, Value, Empty, Empty, Empty>
        where CompactionFunctions : ICompactionFunctions<Key, Value>
    {
        private readonly CompactionFunctions _functions;

        public LogCompactFunctions(CompactionFunctions functions)
        {
            _functions = functions;
        }

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }
        public void ConcurrentReader(ref Key key, ref Empty input, ref Value value, ref Empty dst) { }
        public bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst) { return _functions.CopyInPlace(ref src, ref dst, null); }
        public void CopyUpdater(ref Key key, ref Empty input, ref Value oldValue, ref Value newValue) { }
        public void InitialUpdater(ref Key key, ref Empty input, ref Value value) { }
        public bool InPlaceUpdater(ref Key key, ref Empty input, ref Value value) { return true; }
        public void ReadCompletionCallback(ref Key key, ref Empty input, ref Empty output, Empty ctx, Status status) { }
        public void RMWCompletionCallback(ref Key key, ref Empty input, Empty ctx, Status status) { }
        public void SingleReader(ref Key key, ref Empty input, ref Value value, ref Empty dst) { }
        public void SingleWriter(ref Key key, ref Value src, ref Value dst) { _functions.Copy(ref src, ref dst, null); }
        public void UpsertCompletionCallback(ref Key key, ref Value value, Empty ctx) { }
        public void DeleteCompletionCallback(ref Key key, Empty ctx) { }
    }

    internal unsafe struct DefaultVariableCompactionFunctions<Key, Value> : ICompactionFunctions<Key, Value>
    {
        public void Copy(ref Value src, ref Value dst, IVariableLengthStruct<Value> valueLength)
        {
            var srcLength = valueLength.GetLength(ref src);
            Buffer.MemoryCopy(
                Unsafe.AsPointer(ref src),
                Unsafe.AsPointer(ref dst),
                srcLength,
                srcLength);
        }

        public bool CopyInPlace(ref Value src, ref Value dst, IVariableLengthStruct<Value> valueLength)
        {
            var srcLength = valueLength.GetLength(ref src);
            var dstLength = valueLength.GetLength(ref dst);
            if (srcLength != dstLength)
                return false;

            Buffer.MemoryCopy(
                Unsafe.AsPointer(ref src),
                Unsafe.AsPointer(ref dst),
                dstLength,
                srcLength);

            return true;
        }

        public bool IsDeleted(in Key key, in Value value)
        {
            return false;
        }
    }

    internal struct DefaultCompactionFunctions<Key, Value> : ICompactionFunctions<Key, Value>
    {
        public void Copy(ref Value src, ref Value dst, IVariableLengthStruct<Value> valueLength)
        {
            dst = src;
        }

        public bool CopyInPlace(ref Value src, ref Value dst, IVariableLengthStruct<Value> valueLength)
        {
            dst = src;
            return true;
        }

        public bool IsDeleted(in Key key, in Value value)
        {
            return false;
        }
    }
}