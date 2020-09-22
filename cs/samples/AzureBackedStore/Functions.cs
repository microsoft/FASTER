// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace AzureBackedStore
{
    public class Functions : SimpleFunctions<long, string, string>
    {
        public override void ReadCompletionCallback(ref long key, ref string input, ref string output, ref string ctx, Status status)
        {
            if (status == Status.OK && output == ctx)
                Console.WriteLine("Success!");
            else
                Console.WriteLine("Error!");
        }
    }
}
