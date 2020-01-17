// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.IO;

namespace VarLenStructSample
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

        static void Main()
        {
            Sample1();
            Console.WriteLine("Press <ENTER> to end");
            Console.ReadLine();
        }

        static unsafe void Sample1()
        {
            FasterKV<VarLenType, VarLenType, int[], int[], Empty, VarLenFunctions> fht;
            IDevice log;
            log = Devices.CreateLogDevice("hlog.log", deleteOnClose: true);
            fht = new FasterKV<VarLenType, VarLenType, int[], int[], Empty, VarLenFunctions>
                (128, new VarLenFunctions(),
                new LogSettings { LogDevice = log, MemorySizeBits = 17, PageSizeBits = 12 },
                null, null,
                new VarLenTypeComparer(),
                new VariableLengthStructSettings<VarLenType, VarLenType>
                { keyLength = new VarLenLength(), valueLength = new VarLenLength() }
                );
            var s = fht.NewSession();


            int[] input = default;

            Random r = new Random(100);

            for (int i = 0; i < 5000; i++)
            {
                if (i == 2968)
                { }
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

                s.Upsert(ref key1, ref value, Empty.Default, 0);
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
                var status = s.Read(ref key1, ref input, ref output, Empty.Default, 0);

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
                Console.WriteLine("Sample1: Success!");
            }
            else
            {
                Console.WriteLine("Sample1: Error!");
            }

            s.Dispose();
            fht.Dispose();
            fht = null;
            log.Close();
        }
    }
}