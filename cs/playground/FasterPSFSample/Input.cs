// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FasterPSFSample
{
    public interface IInput<TValue>
    {
        TValue InitialUpdateValue { get; set; }

        int IPUColorInt { get; set; }
    }

    public struct Input<TValue> : IInput<TValue>
    {
        public TValue InitialUpdateValue { get; set; }

        public int IPUColorInt { get; set; }
    }
}
