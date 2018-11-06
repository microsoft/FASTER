// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NestedTypesTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var log = FasterFactory.CreateLogDevice(Path.GetTempPath() + "hybridlog.log");
            var h = FasterFactory.Create
                <
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
                , MyFunctions>
                (128, new MyFunctions(),
                new LogSettings { LogDevice = log, MemorySizeBits = 14, PageSizeBits = 10 }
                );

            h.StartSession();

            for (int i = 0; i < 20000; i++)
            {
                var key =
#if BLIT_KEY && GENERIC_BLIT_KEY
                    new CompoundGroupKey<Empty, TimeKey<int>> {  /*= i*/ }
#else
                    new MyKey { key = i, }
#endif
                    ;
                var value =
#if BLIT_VALUE && GENERIC_BLIT_VALUE
                    new WrappedState<long> { state = i }
#else
                    new MyValue { value = i, }
#endif
                    ;
                h.Upsert(key, value, default(MyContext), 0);
                if (i % 32 == 0) h.Refresh();
            }

            var key1 =
#if BLIT_KEY && GENERIC_BLIT_KEY
                    new CompoundGroupKey<Empty, TimeKey<int>> { /*field = 23*/ }
#else
                    new MyKey { key = 23, }
#endif
                    ;
            var input1 =
#if BLIT_INPUT && GENERIC_BLIT_INPUT
                    new WrappedInput<int, long>()
#else
                    new MyInput()
#endif
                    ;
#if BLIT_OUTPUT && GENERIC_BLIT_OUTPUT
            WrappedState<long> g1 = new WrappedState<long>();
#else
            MyOutput g1 = new MyOutput();
#endif

            h.Read(key1, input1, ref g1, new MyContext(), 0);

            h.CompletePending(true);

            var key2 =
#if BLIT_KEY && GENERIC_BLIT_KEY
                    new CompoundGroupKey<Empty, TimeKey<int>> { /*field = 46*/ }
#else
                    new MyKey { key = 46, }
#endif
                    ;
            var input2 =
#if BLIT_INPUT && GENERIC_BLIT_INPUT
                    new WrappedInput<int, long>()
#else
                    new MyInput()
#endif
                    ;
#if BLIT_OUTPUT && GENERIC_BLIT_OUTPUT
            WrappedState<long> g2 = new WrappedState<long>();
#else
            MyOutput g2 = new MyOutput();
#endif
            h.Read(key2, input2, ref g2, new MyContext(), 0);

            h.CompletePending(true);

            Console.WriteLine("Success!");
            Console.ReadLine();
        }
    }
}
