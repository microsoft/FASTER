// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FASTER.core
{
    [FASTER.core.Roslyn.TypeKind("user")]
    public unsafe class MixedManagedFast
        :
        IManagedFAST<MixedKey, MixedValue, MixedInput, MixedOutput, MixedContext>
    {
        private IFASTER_Mixed store;
        public long Size => store.Size;

        public MixedManagedFast(long size, IDevice logDevice, string checkpointDir, MixedUserFunctions functions, long LogTotalSizeBytes = 17179869184, double LogMutableFraction = 0.9, int LogPageSizeBits = 25)
        {
            MixedFunctionsWrapper.userFunctions = functions;

            store = HashTableManager.GetFasterHashTable
                <MixedKeyWrapper, MixedValueWrapper, MixedInputWrapper,
                MixedOutputWrapper, MixedContextWrapper, MixedFunctionsWrapper,
                IFASTER_Mixed
                >
                (size, logDevice, checkpointDir, LogTotalSizeBytes, LogMutableFraction, LogPageSizeBits);
        }

        public bool CompletePending(bool wait)
        {
            return store.CompletePending(wait);
        }

        public Status Delete(MixedKey key, MixedContext context, long lsn)
        {
            MixedKeyWrapper* keyWrapper;
            MixedContextWrapper* contextWrapper;
#if BLIT_KEY && !GENERIC_BLIT_KEY
                keyWrapper = (MixedKeyWrapper*)&key;
#elif GENERIC_BLIT_KEY // implies BLIT_KEY
            {
                keyWrapper = (MixedKeyWrapper*)Unsafe.AsPointer(ref key);
            }
#else
            {
                var w = BlittableTypeWrapper.Create(key);
                keyWrapper = (MixedKeyWrapper*)&w;
            }
#endif
#if BLIT_CONTEXT
            {
            contextWrapper = (MixedContextWrapper*)&context;
            }
#else
            {
                var w = BlittableTypeWrapper.Create(context);
                contextWrapper = (MixedContextWrapper*)w.ptr;
            }
#endif
            var ret = store.Delete(keyWrapper, contextWrapper, lsn);

            if (ret == Status.OK)
            {
#if !BLIT_KEY
                {
                    MixedKeyWrapper.Free(keyWrapper);
                }
#endif
#if !BLIT_CONTEXT
                {
                    MixedContextWrapper.Free(contextWrapper);
                }
#endif
            }

            return ret;
        }

        public void DumpDistribution()
        {
            store.DumpDistribution();
        }

        public Guid StartSession()
        {
            return store.StartSession();
        }

        public long ContinueSession(Guid guid)
        {
            return store.ContinueSession(guid);
        }

        public void StopSession()
        {
            store.StopSession();
        }

        public void Refresh()
        {
            store.Refresh();
        }

        public Status Read(MixedKey key, MixedInput input, ref MixedOutput output, MixedContext context, long lsn)
        {
            MixedKeyWrapper* keyWrapper;
            MixedInputWrapper* inputWrapper;
            MixedOutputWrapper* outputWrapper;
            MixedContextWrapper* contextWrapper;
            

#if BLIT_KEY && !GENERIC_BLIT_KEY
            {
                keyWrapper = (MixedKeyWrapper*)&key;
            }
#elif GENERIC_BLIT_KEY // implies BLIT_KEY
            {
                keyWrapper = (MixedKeyWrapper*)Unsafe.AsPointer(ref key);
            }
#else
            {
                var w = BlittableTypeWrapper.Create(key);
                keyWrapper = (MixedKeyWrapper*)&w;
            }
#endif
#if BLIT_INPUT && !GENERIC_BLIT_INPUT
            {
                inputWrapper = (MixedInputWrapper*)&input;
            }
#elif GENERIC_BLIT_INPUT
            {
                inputWrapper = (MixedInputWrapper*)Unsafe.AsPointer(ref input);
            }
#else
            {
                var w = BlittableTypeWrapper.Create(input);
                inputWrapper = (MixedInputWrapper*)w.ptr;
            }
#endif
#if BLIT_OUTPUT && !GENERIC_BLIT_OUTPUT
            MixedOutput localOutput = output;
            {
                outputWrapper = (MixedOutputWrapper*)&localOutput;
            }
#elif GENERIC_BLIT_OUTPUT
            {
                outputWrapper = (MixedOutputWrapper*)Unsafe.AsPointer(ref output);
            }
#else
            {
                var w = BlittableTypeWrapper.Create(output);
                outputWrapper = (MixedOutputWrapper*)w.ptr;
            }
#endif
#if BLIT_CONTEXT
            {
                contextWrapper = (MixedContextWrapper*)&context;
            }
#else
            {
                var w = BlittableTypeWrapper.Create(context);
                contextWrapper = (MixedContextWrapper*)w.ptr;
            }
#endif

            var ret =
            store.Read(
                keyWrapper,
                inputWrapper,
                outputWrapper,
                contextWrapper,
                lsn);

            if (ret == Status.OK)
            {

#if !BLIT_KEY
                {
                    MixedKeyWrapper.Free(keyWrapper);
                }
#endif
#if !BLIT_INPUT
                {
                    MixedInputWrapper.Free(inputWrapper);
                }
#endif

#if BLIT_OUTPUT && !GENERIC_BLIT_OUTPUT
                {
                    output = localOutput;
                }
#else
#if !BLIT_OUTPUT
                {
                    output = ((BlittableTypeWrapper*)(&outputWrapper))->GetObject<MixedOutput>();
                    MixedOutputWrapper.Free(outputWrapper);
                }
#endif
#endif

#if !BLIT_CONTEXT
                {
                    MixedContextWrapper.Free(contextWrapper);
                }
#endif
            }

            return ret;
        }

        public Status RMW(MixedKey key, MixedInput input, MixedContext context, long lsn)
        {
            MixedKeyWrapper* keyWrapper;
            MixedInputWrapper* inputWrapper;
            MixedContextWrapper* contextWrapper;


#if BLIT_KEY && !GENERIC_BLIT_KEY
            {
                keyWrapper = (MixedKeyWrapper*)&key;
            }
#elif GENERIC_BLIT_KEY // implies BLIT_KEY
            {
                keyWrapper = (MixedKeyWrapper*)Unsafe.AsPointer(ref key);
            }
#else
            {
                var w = BlittableTypeWrapper.Create(key);
                keyWrapper = (MixedKeyWrapper*)&w;
            }
#endif
#if BLIT_INPUT && !GENERIC_BLIT_INPUT
            {
                inputWrapper = (MixedInputWrapper*)&input;
            }
#elif GENERIC_BLIT_INPUT
            {
                inputWrapper = (MixedInputWrapper*)Unsafe.AsPointer(ref input);
            }
#else
            {
                var w = BlittableTypeWrapper.Create(input);
                inputWrapper = (MixedInputWrapper*)w.ptr;

            }
#endif
#if BLIT_CONTEXT
            {
                contextWrapper = (MixedContextWrapper*)&context;
            }
#else
            {
                var w = BlittableTypeWrapper.Create(context);
                contextWrapper = (MixedContextWrapper*)w.ptr;
            }
#endif

            var ret =
                store.RMW(
                keyWrapper,
                inputWrapper,
                contextWrapper,
                lsn);

            if (ret == Status.OK)
            {
#if !BLIT_KEY
                {
                    MixedKeyWrapper.Free(keyWrapper);
                }
#endif
#if !BLIT_INPUT
                {
                    MixedInputWrapper.Free(inputWrapper);
                }
#endif
#if !BLIT_CONTEXT
                {
                    MixedContextWrapper.Free(contextWrapper);
                }
#endif
            }

            return ret;
        }

        public Status Upsert(MixedKey key, MixedValue value, MixedContext context, long lsn)
        {
            MixedKeyWrapper* keyWrapper;
            MixedValueWrapper* valueWrapper;
            MixedContextWrapper* contextWrapper;

#if BLIT_KEY && !GENERIC_BLIT_KEY
            {
                keyWrapper = (MixedKeyWrapper*)&key;
            }
#elif GENERIC_BLIT_KEY // implies BLIT_KEY
            {
                keyWrapper = (MixedKeyWrapper*)Unsafe.AsPointer(ref key);
            }
#else
            {
                var w = BlittableTypeWrapper.Create(key);
                keyWrapper = (MixedKeyWrapper*)&w;
            }
#endif
#if BLIT_VALUE && !GENERIC_BLIT_VALUE
            {
                valueWrapper = (MixedValueWrapper*)&value;
            }
#elif GENERIC_BLIT_VALUE
            {
                valueWrapper = (MixedValueWrapper*)Unsafe.AsPointer(ref value);
            }
#else
            {
                var w = BlittableTypeWrapper.Create(value);
                valueWrapper = (MixedValueWrapper*)&w;

            }
#endif
#if BLIT_CONTEXT
            {
                contextWrapper = (MixedContextWrapper*)&context;
            }
#else
            {
                var w = BlittableTypeWrapper.Create(context);
                contextWrapper = (MixedContextWrapper*)w.ptr;
            }
#endif

            var ret =
                store.Upsert(
                    keyWrapper,
                    valueWrapper,
                    contextWrapper,
                    lsn);

            if (ret == Status.OK)
            {
#if !BLIT_KEY
                {
                    MixedKeyWrapper.Free(keyWrapper);
                }
#endif
#if !BLIT_VALUE
                {
                    MixedValueWrapper.Free(valueWrapper);
                }
#endif
#if !BLIT_CONTEXT
                {
                    MixedContextWrapper.Free(contextWrapper);
                }
#endif
            }

            return ret;
        }
    }

    [FASTER.core.Roslyn.TypeKind("user")]
    public unsafe static class UserType
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref MixedKey Convert(MixedKeyWrapper* k)
        {
#if BLIT_KEY && !GENERIC_BLIT_KEY
            return ref k->key;
#elif BLIT_KEY && GENERIC_BLIT_KEY
            return ref Unsafe.AsRef<MixedKey>(k);
#else
            return ref ((BlittableTypeWrapper*)k)->GetObject<MixedKey>();
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref MixedValue Convert(MixedValueWrapper* v)
        {
#if BLIT_VALUE && !GENERIC_BLIT_VALUE
            return ref v->value;
#elif BLIT_VALUE && GENERIC_BLIT_VALUE
            return ref Unsafe.AsRef<MixedValue>(v);
#else
            return ref ((BlittableTypeWrapper*)v)->GetObject<MixedValue>();
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref MixedInput Convert(MixedInputWrapper* i)
        {
#if BLIT_INPUT && !GENERIC_BLIT_INPUT
            return ref i->input;
#elif BLIT_INPUT && GENERIC_BLIT_INPUT
            return ref Unsafe.AsRef<MixedInput>(i);
#else
            return ref ((BlittableTypeWrapper*)&i)->GetObject<MixedInput>();
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref MixedOutput Convert(MixedOutputWrapper* o)
        {
#if BLIT_OUTPUT && !GENERIC_BLIT_OUTPUT
            return ref o->output;
#elif BLIT_OUTPUT && GENERIC_BLIT_OUTPUT
            return ref Unsafe.AsRef<MixedOutput>(o);
#else
            return ref ((BlittableTypeWrapper*)&o)->GetObject<MixedOutput>();
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref MixedContext Convert(MixedContextWrapper* c)
        {
#if BLIT_CONTEXT && !GENERIC_BLIT_CONTEXT
            return ref c->context;
#elif BLIT_CONTEXT && GENERIC_BLIT_CONTEXT
            return ref Unsafe.AsRef<MixedContext>(c);
#else
            return ref ((BlittableTypeWrapper*)&c)->GetObject<MixedContext>();
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FreeFromContext(MixedKeyWrapper* k)
        {
#if BLIT_KEY
            MallocFixedPageSize<MixedKeyWrapper>.PhysicalInstance.FreeAtEpoch((long)k);
#else
            ((BlittableTypeWrapper*)k)->Free<MixedKey>();
            MallocFixedPageSize<long>.PhysicalInstance.FreeAtEpoch((long)k);
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FreeFromContext(MixedValueWrapper* v)
        {
#if BLIT_VALUE
            MallocFixedPageSize<MixedValueWrapper>.PhysicalInstance.FreeAtEpoch((long)v);
#else
            ((BlittableTypeWrapper*)v)->Free<MixedValue>();
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FreeFromContext(MixedInputWrapper* i)
        {
#if BLIT_INPUT
            MallocFixedPageSize<MixedInputWrapper>.PhysicalInstance.FreeAtEpoch((long)i);
#else
            ((BlittableTypeWrapper*)&i)->Free<MixedInput>();
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FreeFromContext(MixedOutputWrapper* o)
        {
#if BLIT_OUTPUT
            MallocFixedPageSize<MixedOutputWrapper>.PhysicalInstance.FreeAtEpoch((long)o);
#else
            ((BlittableTypeWrapper*)&o)->Free<MixedOutput>();
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void FreeFromContext(MixedContextWrapper* c)
        {
#if BLIT_CONTEXT
            MallocFixedPageSize<MixedContextWrapper>.PhysicalInstance.FreeAtEpoch((long)c);
#else
            ((BlittableTypeWrapper*)&c)->Free<MixedContext>();
#endif
        }

    }

}
