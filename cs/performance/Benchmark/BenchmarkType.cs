// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace FASTER.Benchmark
{
    enum BenchmarkType : int
    {
        Ycsb,
        ConcurrentDictionaryYcsb
    };
}
