using System;
using FASTER.client;
using FASTER.common;

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace YcsbClient
{
    /// <summary>
    /// Callback functions
    /// </summary>
    public class Functions : CallbackFunctionsBase<long, long, long, long, byte>
    {
        public override void ReadCompletionCallback(ref long key, ref long input, ref long output, byte ctx, Status status)
        {
            if (ctx == 0)
            {
                if (status != Status.OK || key + 10000 != output)
                    throw new Exception("Incorrect read result");
            }
            else if (ctx == 1)
            {
                if (status != Status.OK || key + 10000 + 25 + 25 != output)
                    throw new Exception("Incorrect read result");
            }
            else
            {
                throw new Exception("Unexpected user context");
            }
        }
    }
}
