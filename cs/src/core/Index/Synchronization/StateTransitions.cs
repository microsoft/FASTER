// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

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
        WAIT_INDEX_CHECKPOINT,
        WAIT_FLUSH,
        PERSISTENCE_CALLBACK, 
        REST,
        PREP_INDEX_CHECKPOINT,
        WAIT_INDEX_ONLY_CHECKPOINT,
        PREPARE,
        PREPARE_GROW, 
        IN_PROGRESS_GROW, 
        INTERMEDIATE = 16,
    };

    [StructLayout(LayoutKind.Explicit, Size = 8)]
    internal struct SystemState
    {
        [FieldOffset(0)]
        public Phase phase;

        [FieldOffset(4)]
        public int version;
        
        [FieldOffset(0)]
        public long word;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SystemState Copy(ref SystemState other)
        {
            var info = default(SystemState);
            info.word = other.word;
            return info;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SystemState Make(Phase status, int version)
        {
            var info = default(SystemState);
            info.phase = status;
            info.version = version;
            return info;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SystemState MakeIntermediate(SystemState state) 
            => Make(state.phase | Phase.INTERMEDIATE, state.version);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void RemoveIntermediate(ref SystemState state)
        {
            state.phase &= ~Phase.INTERMEDIATE;
        }

        public static bool Equal(SystemState s1, SystemState s2)
        {
            return s1.word == s2.word;
        }

        public override string ToString()
        {
            return $"[{phase},{version}]";
        }
    }
}
