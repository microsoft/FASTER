// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using FASTER.core;
using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

#pragma warning disable 0649 // Field 'field' is never assigned to, and will always have its default value 'value'; happens due to [Params(..)] 

namespace BenchmarkDotNetTests
{
    [GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory, BenchmarkLogicalGroupRule.ByParams)]
    public class CSharpUnsafe
    {
        [Params(100, 1_000_000)]
        public int NumOps;

        [BenchmarkCategory("RecordInfo.AsRef"), Benchmark(Baseline = true)]
        public void RecordInfoAssignCopy()
        {
            RecordInfo recordInfo = default;
            int counter = 0;
            for (long ii = 0; ii < NumOps; ++ii)
            {
                AssignCopy(ref recordInfo, out RecordInfo result);
                if (result.Valid)
                    ++counter;
            }
        }

        private static void AssignCopy(ref RecordInfo recordInfo, out RecordInfo result) => result = recordInfo;

        [BenchmarkCategory("RecordInfo.AsRef"), Benchmark]
        public void RecordInfoAssignAsPtr()
        {
            RecordInfo recordInfo = default;
            int counter = 0;
            for (long ii = 0; ii < NumOps; ++ii)
            {
                AssignAsPointer(ref recordInfo, out IntPtr recordInfoPtr);
                //ref RecordInfo result = ref Unsafe.AsRef<RecordInfo>((void*) recordInfoPtr);
                //if (result.Valid)
                if (recordInfoPtr != IntPtr.Zero)
                    ++counter;
            }
        }

        private static unsafe void AssignAsPointer(ref RecordInfo recordInfo, out IntPtr recordInfoPtr) => recordInfoPtr = (IntPtr)Unsafe.AsPointer(ref recordInfo);
    }
}
