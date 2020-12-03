// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.common
{
    public unsafe interface IServerSerializer<Key, Value, Input, Output>
    {
        bool Write(ref Output o, ref byte* dst, int length);
        ref Key ReadKeyByRef(ref byte* src);
        ref Value ReadValueByRef(ref byte* src);
        ref Input ReadInputByRef(ref byte* src);
        ref Output AsRefOutput(byte* src, int length);
        void SkipOutput(ref byte* src);
    }
}