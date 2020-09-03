// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace StoreVarLenTypes
{
    public class Program
    {
        // This sample represents an unsafe advanced use of FASTER to store and index variable
        // length keys and/or values without a separate object log. The basic idea is to use 
        // "ref struct" as a proxy for pointers to variable-sized memory in C#. These objects are
        // placed contiguously in the single hybrid log, leading to efficient packing while
        // avoiding the additional I/O (on reads and writes) that a separate object log entails.
        // Users provide information on the actual length of the data underlying the types, by
        // providing implementations for an IVariableLengthStruct<T> interface. Serializers are not
        // required, as these are effectively value-types. One may provide safe APIs on top of 
        // this raw functionality using, for example, Span<T> and Memory<T>.

        static unsafe void Main()
        {
            // VarLen types do not need an object log
            var log = Devices.CreateLogDevice("hlog.log", deleteOnClose: true);

            // Create store, you provide VariableLengthStructSettings for VarLen types
            var store = new FasterKV<VarLenType, VarLenType>(
                size: 1L << 20,
                logSettings: new LogSettings { LogDevice = log, MemorySizeBits = 17, PageSizeBits = 12 },
                comparer: new VarLenTypeComparer(),
                variableLengthStructSettings: new VariableLengthStructSettings<VarLenType, VarLenType>
                    { keyLength = new VarLenLength(), valueLength = new VarLenLength() }
                );

            // Create session
            var s = store.For(new VarLenFunctions()).NewSession<VarLenFunctions>();

            Random r = new Random(100);

            for (int i = 0; i < 5000; i++)
            {
                var keylen = 2 + r.Next(10);
                int* keyval = stackalloc int[keylen];
                ref VarLenType key1 = ref *(VarLenType*)keyval;
                key1.length = keylen;
                for (int j = 1; j < keylen; j++)
                    *(keyval + j) = i;

                var len = 2 + r.Next(10);
                int* val = stackalloc int[len];
                ref VarLenType value = ref *(VarLenType*)val;
                for (int j = 0; j < len; j++)
                    *(val + j) = len;

                s.Upsert(ref key1, ref value);
            }

            bool success = true;
            r = new Random(100);
            for (int i = 0; i < 5000; i++)
            {
                var keylen = 2 + r.Next(10);
                int* keyval = stackalloc int[keylen];
                ref VarLenType key1 = ref *(VarLenType*)keyval;
                key1.length = keylen;
                for (int j = 1; j < keylen; j++)
                    *(keyval + j) = i;

                var len = 2 + r.Next(10);

                int[] output = null;
                var status = s.Read(ref key1, ref output);

                if (status == Status.PENDING)
                {
                    s.CompletePending(true);
                }
                else
                {
                    if ((status != Status.OK) || (output.Length != len))
                    {
                        success = false;
                        break;
                    }

                    for (int j = 0; j < len; j++)
                    {
                        if (output[j] != len)
                        {
                            success = false;
                            break;
                        }
                    }
                }
            }
            if (success)
            {
                Console.WriteLine("Success!");
            }
            else
            {
                Console.WriteLine("Error!");
            }

            s.Dispose();
            store.Dispose();
            log.Close();

            Console.WriteLine("Press <ENTER> to end");
            Console.ReadLine();
        }
    }
}