// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;
using Performance.Common;

namespace FASTER.PerfTest
{
    [JsonObject(MemberSerialization.OptIn)]
    internal class TestOutputs
    {
        [JsonProperty]
        public OperationResults InitialInserts { get; set; } = new OperationResults();

        [JsonProperty]
        public OperationResults TotalOperations { get; set; } = new OperationResults();

        [JsonProperty]
        public OperationResults Upserts { get; set; } = new OperationResults();

        [JsonProperty]
        public OperationResults Reads { get; set; } = new OperationResults();

        [JsonProperty]
        public OperationResults RMWs { get; set; } = new OperationResults();
    }
}
