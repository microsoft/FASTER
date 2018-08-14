// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace ManagedSample2
{
    public class CustomFunctions : IUserFunctions<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty>
    {
        public void RMWCompletionCallback(Empty ctx)
        {
        }

        public void ReadCompletionCallback(Empty ctx, OutputStruct output)
        {
        }

        public void UpsertCompletionCallback(Empty ctx)
        {
        }

        public void CopyUpdater(KeyStruct key, InputStruct input, ValueStruct oldValue, ref ValueStruct newValue)
        {
        }

        public int InitialValueLength(KeyStruct key, InputStruct input)
        {
            return sizeof(int) + sizeof(int);
        }

        public void InitialUpdater(KeyStruct key, InputStruct input, ref ValueStruct value)
        {
            value.vfield1 = input.ifield1;
            value.vfield2 = input.ifield2;
        }

        public void InPlaceUpdater(KeyStruct key, InputStruct input, ref ValueStruct value)
        {
            value.vfield1 += input.ifield1;
            value.vfield2 += input.ifield2;
        }

        public void Reader(KeyStruct key, InputStruct input, ValueStruct value, ref OutputStruct dst)
        {
            dst.value = value;
        }
    }
}
