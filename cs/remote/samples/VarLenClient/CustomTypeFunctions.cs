// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using FASTER.client;

namespace VarLenClient
{
    /// <summary>
    /// Callback functions
    /// </summary>
    public class CustomTypeFunctions : CallbackFunctionsBase<CustomType, CustomType, CustomType, CustomType, byte>
    {
        public override void ReadCompletionCallback(ref CustomType key, ref CustomType input, ref CustomType output, byte ctx, Status status)
        {
            if (ctx == 0)
            {
                if (status != Status.OK || key.payload + 10000 != output.payload)
                    throw new Exception("Incorrect read result");
            }
            else if (ctx == 1)
            {
                if (status != Status.OK || key.payload + 10000 + 25 + 25 != output.payload)
                    throw new Exception("Incorrect read result");
            }
            else
            {
                throw new Exception("Unexpected user context");
            }
        }
    }
}
