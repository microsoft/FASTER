// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    internal struct PSFReadArgs<TKey, TValue>
    {
        internal readonly IPSFInput<TKey> Input;
        internal readonly IPSFOutput<TKey, TValue> Output;

        internal PSFReadArgs(IPSFInput<TKey> input, IPSFOutput<TKey, TValue> output)
        {
            this.Input = input;
            this.Output = output;
        }
    }
}
