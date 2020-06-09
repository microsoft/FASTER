// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;

namespace Performance.Common
{
    [JsonObject(MemberSerialization.OptIn)]
    internal class OperationResults
    {
        [JsonProperty]
        internal ResultStats AllThreadsFull { get; set; } = new ResultStats();

        [JsonProperty]
        internal ResultStats AllThreadsTrimmed { get; set; } = new ResultStats();

        [JsonProperty]
        internal ResultStats PerThreadFull { get; set; } = new ResultStats();

        [JsonProperty]
        internal ResultStats PerThreadTrimmed { get; set; } = new ResultStats();
    }
}
