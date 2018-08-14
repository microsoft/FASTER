// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace NestedTypesTest
{
    public class MyFunctions : IUserFunctions<
#if BLIT_KEY && GENERIC_BLIT_KEY
                CompoundGroupKey<Empty, TimeKey<int>>
#else
                MyKey
#endif
                ,
#if BLIT_VALUE && GENERIC_BLIT_VALUE
                WrappedState<long>
#else
                MyValue
#endif
                ,
#if BLIT_INPUT && GENERIC_BLIT_INPUT
                WrappedInput<int, long>
#else
                MyInput
#endif
                ,
#if BLIT_INPUT && GENERIC_BLIT_INPUT
                WrappedState<long>
#else
                MyOutput
#endif
                ,
#if BLIT_CONTEXT && GENERIC_BLIT_CONTEXT
                
#else
                MyContext
#endif
        >
    {
        public void RMWCompletionCallback(MyContext ctx)
        {
        }

        public void ReadCompletionCallback(MyContext ctx,
#if BLIT_OUTPUT && GENERIC_BLIT_OUTPUT
            WrappedState<long> output
#else
            MyOutput
#endif
            )
        {
        }
        public void UpsertCompletionCallback(MyContext ctx)
        {
        }

        public void CopyUpdater(
#if BLIT_KEY && GENERIC_BLIT_KEY
            CompoundGroupKey<Empty, TimeKey<int>>
#else
            MyKey
#endif
            key,
#if BLIT_INPUT && GENERIC_BLIT_INPUT
            WrappedInput<int, long>
#else
            MyInput
#endif
            input,
#if BLIT_VALUE && GENERIC_BLIT_VALUE
            WrappedState<long> oldValue,
            ref WrappedState<long> newValue
#else
            MyValue oldValue,
            ref MyValue newValue
#endif
            )
        {
        }

        public int InitialValueLength(CompoundGroupKey<Empty, TimeKey<int>> key, WrappedInput<int, long> input)
        {
            return sizeof(int) + sizeof(int);
        }

        public void InitialUpdater(
#if BLIT_KEY && GENERIC_BLIT_KEY
            CompoundGroupKey<Empty, TimeKey<int>>
#else
            MyKey
#endif
            key,
#if BLIT_INPUT && GENERIC_BLIT_INPUT
            WrappedInput<int, long>
#else
            MyInput
#endif
            input,
            ref
#if BLIT_VALUE && GENERIC_BLIT_VALUE
            WrappedState<long>
#else
            MyValue
#endif
            value
            )
        {
        }

        public void InPlaceUpdater(
#if BLIT_KEY && GENERIC_BLIT_KEY
            CompoundGroupKey<Empty, TimeKey<int>>
#else
            MyKey
#endif
            key,
#if BLIT_INPUT && GENERIC_BLIT_INPUT
            WrappedInput<int, long>
#else
            MyInput
#endif
            input,
            ref
#if BLIT_VALUE && GENERIC_BLIT_VALUE
            WrappedState<long>
#else
            MyValue
#endif
            value)
        {
        }

        public void Reader(
#if BLIT_KEY && GENERIC_BLIT_KEY
            CompoundGroupKey<Empty, TimeKey<int>>
#else
            MyKey
#endif
            key,
#if BLIT_INPUT && GENERIC_BLIT_INPUT
            WrappedInput<int, long>
#else
            MyInput
#endif
            input,
#if BLIT_VALUE && GENERIC_BLIT_VALUE
            WrappedState<long>
#else
            MyValue
#endif
            value,
            ref
#if BLIT_OUTPUT && GENERIC_BLIT_OUTPUT
            WrappedState<long>
#else
            MyOutput
#endif
            dst)
        {
            //dst.value = value;
        }
    }
}
