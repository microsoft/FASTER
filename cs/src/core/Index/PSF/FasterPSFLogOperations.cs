// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;

namespace FASTER.core
{
    public partial class FasterKV<Key, Value, Input, Output, Context, Functions> : FasterBase,
        IFasterKV<Key, Value, Input, Output, Context, Functions>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        /// <inheritdoc/>
        public void FlushPSFLogs(bool wait) => this.PSFManager.FlushLogs(wait);

        /// <inheritdoc/>
        public bool FlushAndEvictPSFLogs(bool wait) => this.PSFManager.FlushAndEvictLogs(wait);

        /// <inheritdoc/>
        public void DisposePSFLogsFromMemory() => this.PSFManager.DisposeLogsFromMemory();
    }
}
