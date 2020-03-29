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
    internal unsafe struct ResizeInfo
    {
        [FieldOffset(0)]
        public ResizeOperationStatus status;

        [FieldOffset(4)]
        public int version;

        [FieldOffset(0)]
        public long word;
    }

    /// <summary>
    /// </summary>
    public enum Phase : int {
        /// <summary>
        /// </summary>
        IN_PROGRESS, 
        /// <summary>
        /// </summary>
        WAIT_PENDING, 
        /// <summary>
        /// </summary>
        WAIT_FLUSH, 
        /// <summary>
        /// </summary>
        PERSISTENCE_CALLBACK, 
        /// <summary>
        /// </summary>
        WAIT_INDEX_CHECKPOINT,
        /// <summary>
        /// </summary>
        REST,
        /// <summary>
        /// </summary>
        PREP_INDEX_CHECKPOINT, 
        /// <summary>
        /// </summary>
        INDEX_CHECKPOINT, 
        /// <summary>
        /// </summary>
        PREPARE,
        /// <summary>
        /// </summary>
        PREPARE_GROW, 
        /// <summary>
        /// </summary>
        IN_PROGRESS_GROW, 
        /// <summary>
        /// </summary>
        INTERMEDIATE,
    };

    /// <summary>
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct SystemState
    {
        /// <summary>
        /// </summary>
        [FieldOffset(0)]
        public Phase phase;

        /// <summary>
        /// </summary>
        [FieldOffset(4)]
        public int version;

        /// <summary>
        /// </summary>
        [FieldOffset(0)]
        public long word;

        /// <summary>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SystemState Copy(ref SystemState other)
        {
            var info = default(SystemState);
            info.word = other.word;
            return info;
        }

        /// <summary>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SystemState Make(Phase status, int version)
        {
            var info = default(SystemState);
            info.phase = status;
            info.version = version;
            return info;
        }
    }
}
