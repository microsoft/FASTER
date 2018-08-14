// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace FASTER.core
{

    [FASTER.core.Roslyn.TypeKind("internal")]
#if BLIT_OUTPUT
    public unsafe struct MixedOutputWrapper
    {
        public MixedOutput output;

        public static MixedOutputWrapper* MoveToContext(MixedOutputWrapper* output)
        {
            var addr = (MixedOutputWrapper*)
                MallocFixedPageSize<MixedOutputWrapper>.PhysicalInstance.Allocate();
            return addr;
        }
    }
#else
    public unsafe struct MixedOutputWrapper
    {
        public BlittableTypeWrapper output;

        public static void Free(MixedOutputWrapper* output)
        {
            ((BlittableTypeWrapper*)(&output))->Free<MixedOutput>();
        }

        public static MixedOutputWrapper* MoveToContext(MixedOutputWrapper* output)
        {
            return output;
        }
    }
#endif
}
