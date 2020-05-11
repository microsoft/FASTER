// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace FasterPSFSample
{
    public class Context<TValue>
    {
        public List<TValue> Value { get; set; } = new List<TValue>();
    }
}
