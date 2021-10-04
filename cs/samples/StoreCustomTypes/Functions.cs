// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace StoreCustomTypes
{
    public sealed class Functions : FunctionsBase<MyKey, MyValue, MyInput, MyOutput, MyContext>
    {
        public override void InitialUpdater(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput output) => value.value = input.value;
        public override void CopyUpdater(ref MyKey key, ref MyInput input, ref MyValue oldValue, ref MyValue newValue, ref MyOutput output, ref RecordInfo recordInfo, long address) => newValue = oldValue;
        public override bool InPlaceUpdater(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput output, ref RecordInfo recordInfo, long address) { value.value += input.value; return true; }


        public override bool SingleReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst, long address)
        {
            dst.value = value;
            return true;
        }

        public override bool ConcurrentReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst, ref RecordInfo recordInfo, long address)
        {
            dst.value = value;
            return true;
        }

        public override void ReadCompletionCallback(ref MyKey key, ref MyInput input, ref MyOutput output, MyContext ctx, Status status, RecordInfo recordInfo)
        {
            if (output.value.value == key.key)
                Console.WriteLine("Success!");
            else
                Console.WriteLine("Error!");
        }
    }
}
