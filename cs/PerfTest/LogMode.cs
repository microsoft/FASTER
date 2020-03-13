// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace PerfTest
{
    [JsonConverter(typeof(StringEnumConverter))]
    public enum LogMode
    {
        Flush,
        FlushAndEvict,
        DisposeFromMemory
    };
}
