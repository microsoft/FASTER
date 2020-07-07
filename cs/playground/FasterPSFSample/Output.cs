// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FasterPSFSample
{
    public interface IOutput<TValue>
    {
        TValue Value { get; set; }
    }

    public struct Output<TValue> : IOutput<TValue>
    {
        public TValue Value { get; set; }
    }
}
