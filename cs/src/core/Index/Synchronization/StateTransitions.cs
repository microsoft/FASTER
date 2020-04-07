// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    internal enum ResizeOperationStatus : int { IN_PROGRESS, DONE };

    [StructLayout(LayoutKind.Explicit, Size = 8)]
    internal struct ResizeInfo
    {
        [FieldOffset(0)]
        public ResizeOperationStatus status;

        [FieldOffset(4)]
        public int version;

        [FieldOffset(0)]
        public long word;
    }

    internal enum Phase : int {
        IN_PROGRESS, 
        WAIT_PENDING, 
        WAIT_FLUSH, 
        PERSISTENCE_CALLBACK, 
        WAIT_INDEX_CHECKPOINT,
        REST,
        PREP_INDEX_CHECKPOINT, 
        INDEX_CHECKPOINT, 
        PREPARE,
        PREPARE_GROW, 
        IN_PROGRESS_GROW, 
        INTERMEDIATE,
    };

    /// <summary>
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    internal struct SystemState
    {
        [FieldOffset(0)]
        internal Phase phase;

        [FieldOffset(4)]
        internal int version;
        
        [FieldOffset(0)]
        internal long word;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static SystemState Copy(ref SystemState other)
        {
            var info = default(SystemState);
            info.word = other.word;
            return info;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static SystemState Make(Phase status, int version)
        {
            var info = default(SystemState);
            info.phase = status;
            info.version = version;
            return info;
        }
    }
}
