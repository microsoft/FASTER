// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

namespace FASTER.core
{
    [FASTER.core.Roslyn.TypeKind("internal")]
#if BLIT_CONTEXT
    public unsafe struct MixedContextWrapper
    {
        public MixedContext context;

        public static void Copy(MixedContextWrapper* src, MixedContextWrapper* dst)
        {
            dst->context = src->context;
        }

        public static MixedContextWrapper* MoveToContext(MixedContextWrapper* context)
        {
            var addr = (MixedContextWrapper*)
                MallocFixedPageSize<MixedContextWrapper>.PhysicalInstance.Allocate();
            Copy(context, addr);
            return addr;
        }
    }
#else
    public unsafe struct MixedContextWrapper
    {
        public BlittableTypeWrapper context;

        public static MixedContextWrapper* MoveToContext(MixedContextWrapper* context)
        {
            return context;
        }

        public static void Free(MixedContextWrapper* context)
        {
            ((BlittableTypeWrapper*)(&context))->Free<MixedContext>();
        }

    }
#endif
}
