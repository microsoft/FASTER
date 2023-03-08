// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using FASTER.core;

#pragma warning disable 0649 // Field 'field' is never assigned to, and will always have its default value 'value'; happens due to [Params(..)] 

namespace BenchmarkDotNetTests
{
    [GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory, BenchmarkLogicalGroupRule.ByParams)]
    public class LightEpochTests
    {
        [Params(10_000_000, 100_000_000, 1_000_000_000)]
        public int NumIterations;

        [BenchmarkCategory("LightEpoch"), Benchmark]
        public void AcquireAndRelease()
        {
            var epoch = new LightEpoch();
            for (int i = 0; i < NumIterations; i++)
            {
                epoch.Resume();
                epoch.Suspend();
            }
        }
    }
}
