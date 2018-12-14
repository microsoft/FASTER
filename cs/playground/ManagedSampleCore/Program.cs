// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StructSample
{
    public class Program
    {
        static unsafe void Main(string[] args)
        {
            var fht = FasterFactory.Create
                <KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions, ICustomFasterKv>
                (128, new LogSettings { LogDevice = FasterFactory.CreateLogDevice(""), MutableFraction = 0.5 });

            fht.StartSession();

            OutputStruct output = default(OutputStruct);

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            // Upsert item into store, and read it back
            fht.Upsert(&key1, &value, null, 0);
            fht.Read(&key1, null, &output, null, 0);

            if ((output.value.vfield1 != value.vfield1) || (output.value.vfield2 != value.vfield2))
                Console.WriteLine("Error!");
            else
                Console.WriteLine("Success!");

            var key2 = new KeyStruct { kfield1 = 15, kfield2 = 16 };
            var input = new InputStruct { ifield1 = 25, ifield2 = 26 };

            // Two read-modify-write (RMW) operations (sum aggregator)
            // Followed by read of result
            fht.RMW(&key2, &input, null, 0);
            fht.RMW(&key2, &input, null, 0);
            fht.Read(&key2, null, &output, null, 0);

            if ((output.value.vfield1 != input.ifield1*2) || (output.value.vfield2 != input.ifield2*2))
                Console.WriteLine("Error!");
            else
                Console.WriteLine("Success!");

            fht.StopSession();

            Console.ReadLine();
        }
    }
}
