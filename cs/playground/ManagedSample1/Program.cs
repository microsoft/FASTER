// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ManagedSampleCore
{
    public class Program
    {
        static void Main(string[] args)
        {
            // This sample uses the unsafe API of FASTER, and works only for blittable struct types
            // Your structs have to implement certain static methods (see the structs for details)
            // You also define the interface (ICustomFaster) that will be returned by the factory
            // This sample represents the highest performance level for FASTER, at the expense of
            // supporting limited types.

            var fht = 
                new FasterKV<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions>
                (128, new Functions(), new LogSettings { LogDevice = FasterFactory.CreateLogDevice(""), MutableFraction = 0.5 });

            fht.StartSession();

            InputStruct input = default(InputStruct);
            OutputStruct output = default(OutputStruct);
            Empty context = default(Empty);

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            // Upsert item into store, and read it back
            fht.Upsert(ref key1, ref value, ref context, 0);
            fht.Read(ref key1, ref input, ref output, ref context, 0);

            if ((output.value.vfield1 != value.vfield1) || (output.value.vfield2 != value.vfield2))
                Console.WriteLine("Error!");
            else
                Console.WriteLine("Success!");

            var key2 = new KeyStruct { kfield1 = 15, kfield2 = 16 };
            input = new InputStruct { ifield1 = 25, ifield2 = 26 };

            // Two read-modify-write (RMW) operations (sum aggregator)
            // Followed by read of result
            fht.RMW(ref key2, ref input, ref context, 0);
            fht.RMW(ref key2, ref input, ref context, 0);
            fht.Read(ref key2, ref input, ref output, ref context, 0);

            if ((output.value.vfield1 != input.ifield1*2) || (output.value.vfield2 != input.ifield2*2))
                Console.WriteLine("Error!");
            else
                Console.WriteLine("Success!");

            fht.StopSession();

            Console.ReadLine();
        }
    }
}
