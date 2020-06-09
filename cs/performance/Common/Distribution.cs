// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Performance.Common
{
    [JsonConverter(typeof(StringEnumConverter))]
    public enum Distribution
    {
        Uniform,        // Uniformly random distribution of keys
        ZipfSmooth,     // Smooth curve (most localized keys)
        ZipfShuffled    // Shuffle keys after curve generation
    }
}
