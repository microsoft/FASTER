// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json.Converters;
using System.Text.Json.Serialization;

namespace FASTER.PerfTest
{
    [JsonConverter(typeof(StringEnumConverter))]
    public enum ThreadMode
    {
        Sync,
        Affinitized,
        Async
    }
}
