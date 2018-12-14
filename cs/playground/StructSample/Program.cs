// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StructSampleCore
{
    public class Program
    {
        static void Main(string[] args)
        {
            // This sample uses "blittable" key and value types, which enables the 
            // "high speed" mode for FASTER. You can override the default key equality 
            // comparer in two ways;
            // (1) Make Key implement IFasterEqualityComparer<Key> interface
            // (2) Provide IFasterEqualityComparer<KeyStruct> instance as param to constructor
            // Serializers are not required for blittable key and value types.

            var fht = 
                new FasterKV<Key, Value, Input, Output, Empty, Functions>
                (128, new Functions(), new LogSettings { LogDevice = Devices.CreateLogDevice(""), MutableFraction = 0.5 });

            fht.StartSession();

            Input input = default(Input);
            Output output = default(Output);

            var key1 = new Key { kfield1 = 13, kfield2 = 14 };
            var value = new Value { vfield1 = 23, vfield2 = 24 };

            // Upsert item into store, and read it back
            fht.Upsert(ref key1, ref value, Empty.Default, 0);
            fht.Read(ref key1, ref input, ref output, Empty.Default, 0);

            if ((output.value.vfield1 != value.vfield1) || (output.value.vfield2 != value.vfield2))
                Console.WriteLine("Error!");
            else
                Console.WriteLine("Success!");

            var key2 = new Key { kfield1 = 15, kfield2 = 16 };
            input = new Input { ifield1 = 25, ifield2 = 26 };

            // Two read-modify-write (RMW) operations (sum aggregator)
            // Followed by read of result
            fht.RMW(ref key2, ref input, Empty.Default, 0);
            fht.RMW(ref key2, ref input, Empty.Default, 0);
            fht.Read(ref key2, ref input, ref output, Empty.Default, 0);

            if ((output.value.vfield1 != input.ifield1*2) || (output.value.vfield2 != input.ifield2*2))
                Console.WriteLine("Error!");
            else
                Console.WriteLine("Success!");

            fht.StopSession();

            Console.ReadLine();
        }
    }
}
