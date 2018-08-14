// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FASTER.core;

namespace ManagedSample2
{
    class Program
    {
        static void Main(string[] args)
        { 
            var fht = FASTERFactory.Create
                <KeyStruct, ValueStruct, InputStruct, OutputStruct, 
                Empty, CustomFunctions>
                (128, new NullDevice(), new CustomFunctions());

            fht.StartSession();

            Empty context;
            OutputStruct output = new OutputStruct();

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };
            fht.Upsert(key1, value, default(Empty), 0);
            fht.Read(key1, default(InputStruct), ref output, context, 0);

            if ((output.value.vfield1 != value.vfield1) || (output.value.vfield2 != value.vfield2))
                Console.WriteLine("Error!");
            else
                Console.WriteLine("Success!");

            KeyStruct key2 = new KeyStruct { kfield1 = 15, kfield2 = 16 };
            InputStruct input = new InputStruct { ifield1 = 25, ifield2 = 26 };
            fht.RMW(key2, input, context, 0);
            fht.RMW(key2, input, context, 0);
            fht.Read(key2, default(InputStruct), ref output, context, 0);

            if ((output.value.vfield1 != input.ifield1 * 2) || (output.value.vfield2 != input.ifield2 * 2))
                Console.WriteLine("Error!");
            else
                Console.WriteLine("Success!");

            fht.StopSession();

            Console.ReadLine();
        }
    }
}
