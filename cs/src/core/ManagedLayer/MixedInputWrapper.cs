// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace FASTER.core
{
    [FASTER.core.Roslyn.TypeKind("internal")]
#if BLIT_INPUT
    public unsafe struct MixedInputWrapper
    {
        public MixedInput input;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength(MixedInputWrapper* input)
        {
            return sizeof(MixedInputWrapper);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(MixedInputWrapper* src, MixedInputWrapper* dst)
        {
            dst->input = src->input;
        }

        public static MixedInputWrapper* MoveToContext(MixedInputWrapper* input)
        {
            var addr = (MixedInputWrapper*)
                MallocFixedPageSize<MixedInputWrapper>.PhysicalInstance.Allocate();
            Copy(input, addr);
            return addr;
        }
    }
#else
    public unsafe struct MixedInputWrapper
    {
        public BlittableTypeWrapper input;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength(MixedInputWrapper* input)
        {
            return BlittableTypeWrapper.kSize;
        }
        public static void Free(MixedInputWrapper* input)
        {
            ((BlittableTypeWrapper*)(&input))->Free<MixedInput>();
        }

        public static MixedInputWrapper* MoveToContext(MixedInputWrapper* input)
        {
            return input;
        }
    }
#endif
}
