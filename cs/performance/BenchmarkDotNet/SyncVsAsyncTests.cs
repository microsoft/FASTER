// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using FASTER.core;
using System;
using System.IO;
using System.Threading.Tasks;

#pragma warning disable 0649 // Field 'field' is never assigned to, and will always have its default value 'value'; happens due to [Params(..)] 

namespace BenchmarkDotNetTests
{
    [GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory, BenchmarkLogicalGroupRule.ByParams)]
    public class SyncVsAsync
    {
        [Params(100, 1_000_000)]
        public int NumRecords;

        //[ParamsAllValues]
        //bool useAsync;

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

        void PopulateStoreSync()
        {
            using var session = store.For(new SimpleFunctions<long, long>()).NewSession<SimpleFunctions<long, long>>();
            for (long ii = 0; ii < NumRecords; ++ii)
                session.Upsert(ii, ii);
        }

        async ValueTask PopulateStoreAsync()
        {
            using var session = store.For(new SimpleFunctions<long, long>()).NewSession<SimpleFunctions<long, long>>();
            for (long ii = 0; ii < NumRecords; ++ii)
            {
                var result = await session.UpsertAsync(ii, ii);
                while (result.Status.IsPending)
                    result = await result.CompleteAsync().ConfigureAwait(false);
            }
        }

        [GlobalSetup(Targets = new[] { nameof(InsertSync), nameof(InsertAsync) })]
        public void SetupEmptyStore() => SetupStore();

        [GlobalSetup(Targets = new[] { nameof(RMWSync), nameof(RMWAsync), nameof(ReadSync), nameof(ReadAsync) })]
        public void SetupPopulatedStore()
        {
            SetupStore();
            PopulateStoreSync();
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

        [BenchmarkCategory("Insert"), Benchmark(Baseline = true)]
        public void InsertSync() => PopulateStoreSync();

        [BenchmarkCategory("Insert"), Benchmark]
        public ValueTask InsertAsync() => PopulateStoreAsync();

        [BenchmarkCategory("RMW"), Benchmark(Baseline = true)]
        public void RMWSync()
        {
            using var session = store.For(new SimpleFunctions<long, long>()).NewSession<SimpleFunctions<long, long>>();
            for (long ii = 0; ii < NumRecords; ++ii)
                session.RMW(ii, ii * 2);
            session.CompletePending();
        }

        [BenchmarkCategory("RMW"), Benchmark]
        public async ValueTask RMWAsync()
        {
            using var session = store.For(new SimpleFunctions<long, long>()).NewSession<SimpleFunctions<long, long>>();
            for (long ii = 0; ii < NumRecords; ++ii)
            {
                var result = await session.RMWAsync(ii, ii * 2);
                while (result.Status.IsPending)
                    result = await result.CompleteAsync().ConfigureAwait(false);
            }
        }

        [BenchmarkCategory("Read"), Benchmark(Baseline = true)]
        public void ReadSync()
        {
            using var session = store.For(new SimpleFunctions<long, long>()).NewSession<SimpleFunctions<long, long>>();
            for (long ii = 0; ii < NumRecords; ++ii)
            {
                var (status, output) = session.Read(ii);
                if (status.IsPending)
                {
                    session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    completedOutputs.Dispose();
                }
            }
            session.CompletePending();
        }

        [BenchmarkCategory("Read"), Benchmark]
        public async ValueTask ReadAsync()
        {
            using var session = store.For(new SimpleFunctions<long, long>()).NewSession<SimpleFunctions<long, long>>();
            for (long ii = 0; ii < NumRecords; ++ii)
            {
                var (status, output) = (await session.ReadAsync(ii).ConfigureAwait(false)).Complete();
            }
        }
    }
}
