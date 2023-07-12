// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnostics.Windows.Configs;
using FASTER.core;
using System;
using System.IO;

#pragma warning disable 0649 // Field 'field' is never assigned to, and will always have its default value 'value'; happens due to [Params(..)] 

namespace BenchmarkDotNetTests
{
    [InliningDiagnoser(logFailuresOnly: false, allowedNamespaces: new[] { "FASTER.core" })]
    [GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory, BenchmarkLogicalGroupRule.ByParams)]
    public class InliningDiagnoserTests
    {
        [Params(1_000_000)]
        public int NumRecords;

        FasterKV<long, long> store;
        IDevice logDevice;
        string logDirectory;

        void SetupStore()
        {
            logDirectory = BenchmarkDotNetTestsApp.TestDirectory;
            var logFilename = Path.Combine(logDirectory, $"{nameof(SyncVsAsync)}_{Guid.NewGuid()}.log");
            logDevice = Devices.CreateLogDevice(logFilename, preallocateFile: true, deleteOnClose: true, useIoCompletionPort: true);
            var logSettings = new LogSettings
            {
                LogDevice = logDevice
            };
            store = new FasterKV<long, long>(1L << 20, logSettings);
        }

        void PopulateStore()
        {
            using var session = store.For(new SimpleFunctions<long, long>()).NewSession<SimpleFunctions<long, long>>();
            for (long ii = 0; ii < NumRecords; ++ii)
                session.Upsert(ii, ii);
        }

        [GlobalSetup]
        public void SetupPopulatedStore()
        {
            SetupStore();
            PopulateStore();
        }

        [GlobalCleanup]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            logDevice?.Dispose();
            logDevice = null;
            try
            {
                Directory.Delete(logDirectory);
            }
            catch { }
        }

        [BenchmarkCategory("Upsert"), Benchmark]
        public void Upsert()
        {
            using var session = store.For(new SimpleFunctions<long, long>()).NewSession<SimpleFunctions<long, long>>();
            for (long ii = 0; ii < NumRecords; ++ii)
                session.Upsert(ii, ii * 2);
            session.CompletePending();
        }

        [BenchmarkCategory("RMW"), Benchmark]
        public void RMW()
        {
            using var session = store.For(new SimpleFunctions<long, long>()).NewSession<SimpleFunctions<long, long>>();
            for (long ii = 0; ii < NumRecords; ++ii)
                session.RMW(ii, ii * 2);
            session.CompletePending();
        }

        [BenchmarkCategory("Read"), Benchmark]
        public void Read()
        {
            using var session = store.For(new SimpleFunctions<long, long>()).NewSession<SimpleFunctions<long, long>>();
            for (long ii = 0; ii < NumRecords; ++ii)
                session.Read(ii);
            session.CompletePending();
        }
    }
}
