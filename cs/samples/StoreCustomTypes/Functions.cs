// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace StoreCustomTypes
{
    public sealed class Functions : FunctionsBase<MyKey, MyValue, MyInput, MyOutput, MyContext>
    {
        public override void InitialUpdater(ref MyKey key, ref MyInput input, ref MyOutput output, ref MyValue value) => value.value = input.value;
        public override void CopyUpdater(ref MyKey key, ref MyInput input, ref MyOutput output, ref MyValue oldValue, ref MyValue newValue) => newValue = oldValue;
        public override bool InPlaceUpdater(ref MyKey key, ref MyInput input, ref MyOutput output, ref MyValue value) { value.value += input.value; return true; }


        public override void SingleReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst) => dst.value = value;
        public override void ConcurrentReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst) => dst.value = value;

        public override void ReadCompletionCallback(ref MyKey key, ref MyInput input, ref MyOutput output, MyContext ctx, Status status)
        {
            if (output.value.value == key.key)
                Console.WriteLine("Success!");
            else
                Console.WriteLine("Error!");
        }
    }
}
