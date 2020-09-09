// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace StoreCustomTypes
{
    public class Functions : IFunctions<MyKey, MyValue, MyInput, MyOutput, MyContext>
    {
        public void InitialUpdater(ref MyKey key, ref MyInput input, ref MyValue value) => value.value = input.value;
        public void CopyUpdater(ref MyKey key, ref MyInput input, ref MyValue oldValue, ref MyValue newValue) => newValue = oldValue;
        public bool InPlaceUpdater(ref MyKey key, ref MyInput input, ref MyValue value) { value.value += input.value; return true; }


        public void SingleReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst) => dst.value = value;
        public void SingleWriter(ref MyKey key, ref MyValue src, ref MyValue dst) => dst = src;
        public void ConcurrentReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst) => dst.value = value;
        public bool ConcurrentWriter(ref MyKey key, ref MyValue src, ref MyValue dst) { dst = src; return true; }

        public void ReadCompletionCallback(ref MyKey key, ref MyInput input, ref MyOutput output, MyContext ctx, Status status)
        {
            if (output.value.value == key.key)
                Console.WriteLine("Success!");
            else
                Console.WriteLine("Error!");
        }
        public void UpsertCompletionCallback(ref MyKey key, ref MyValue value, MyContext ctx) { }
        public void RMWCompletionCallback(ref MyKey key, ref MyInput input, MyContext ctx, Status status) { }
        public void DeleteCompletionCallback(ref MyKey key, MyContext ctx) { }
        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }
    }
}
