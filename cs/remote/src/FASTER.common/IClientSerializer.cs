// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.common
{
    public unsafe interface IClientSerializer<Key, Value, Input, Output>
    {
        bool Write(ref Key k, ref byte* dst, int length);
        bool Write(ref Value v, ref byte* dst, int length);
        bool Write(ref Input i, ref byte* dst, int length);
        Output ReadOutput(ref byte* src);
    }
}