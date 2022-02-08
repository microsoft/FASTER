// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace StoreCheckpointRecover
{
    public sealed class Functions : FunctionsBase<MyKey, MyValue, MyInput, MyOutput, MyContext>
    {
        public override bool InitialUpdater(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput output, ref RecordInfo recordInfo, ref int usedValueLength, int fullValueLength, long address) { value.value = input.value; return true; }
        public override bool CopyUpdater(ref MyKey key, ref MyInput input, ref MyValue oldValue, ref MyValue newValue, ref MyOutput output, ref RecordInfo recordInfo, ref int usedValueLength, int fullValueLength, long address) { newValue = oldValue; return true; }
        public override bool InPlaceUpdater(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput output, ref RecordInfo recordInfo, ref int usedValueLength, int fullValueLength, long address) { value.value += input.value; return true; }

        public override bool SingleReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst, ref RecordInfo recordInfo, long address)
        { dst = new MyOutput(); dst.value = value; return true; }
        public override bool ConcurrentReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst, ref RecordInfo recordInfo, long address)
        { dst = new MyOutput(); dst.value = value; return true; }

        public override void ReadCompletionCallback(ref MyKey key, ref MyInput input, ref MyOutput output, MyContext ctx, Status status, RecordMetadata recordMetadata)
        {
            if (output.value.value == key.key)
                Console.WriteLine("Success!");
            else
                Console.WriteLine("Error!");
        }
    }
}
