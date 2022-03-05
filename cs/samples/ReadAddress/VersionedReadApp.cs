// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace ReadAddress
{
    class VersionedReadApp
    {
        // Number of keys in store
        const int numKeys = 1000;
        const int keyMod = 100;
        const int maxLap = numKeys / keyMod;
        const int deleteLap = maxLap / 2;

        const string ReadCacheArg = "--readcache";
        static bool useReadCache = false;
        const string CheckpointsArg = "--checkpoints";
        static bool useCheckpoints = true;
        const string RMWArg = "--rmw";
        static bool useRMW = false;

        private static void Usage()
        {
            Console.WriteLine("Reads 'linked lists' of records for each key by backing up the previous-address chain, including showing record versions");
            Console.WriteLine("Usage:");
            Console.WriteLine($"  {ReadCacheArg}: use Read Cache; default = {useReadCache}");
            Console.WriteLine($"  {CheckpointsArg}: issue periodic checkpoints during load; default = {useCheckpoints}");
            Console.WriteLine($"  {RMWArg}: issue periodic checkpoints during load; default = {useRMW}");
        }

        static async Task<int> Main(string[] args)
        {
            for (var ii = 0; ii < args.Length; ++ii)
            {
                var arg = args[ii];
                if (arg.ToLower() == ReadCacheArg)
                {
                    useReadCache = true;
                    continue;
                }
                if (arg.ToLower() == CheckpointsArg)
                {
                    useCheckpoints = true;
                    continue;
                }
                if (arg.ToLower() == RMWArg)
                {
                    useRMW = true;
                    continue;
                }
                Console.WriteLine($"Unknown option: {arg}");
                Usage();
                return -1;
            }

            var (store, log, path) = CreateStore();
            await PopulateStore(store);
            
            const int keyToScan = 42;
            ScanStore(store, keyToScan);
            var cts = new CancellationTokenSource();
            await ScanStoreAsync(store, keyToScan, cts.Token);

            // Clean up
            store.Dispose();
            log.Dispose();

            // Delete the created files
            try { new DirectoryInfo(path).Delete(true); } catch { }

            Console.WriteLine("Press <ENTER> to end");
            Console.ReadLine();
            return 0;
        }

        private static (FasterKV<Key, Value>, IDevice, string) CreateStore()
        {
            var path = Path.GetTempPath() + "FasterReadAddressSample\\";
            var log = Devices.CreateLogDevice(path + "hlog.log");

            var logSettings = new LogSettings
            {
                LogDevice = log,
                ObjectLogDevice = new NullDevice(),
                ReadCacheSettings = useReadCache ? new ReadCacheSettings() : null,
                // Use small-footprint values
                PageSizeBits = 12, // (4K pages)
                MemorySizeBits = 20 // (1M memory for main log)
            };

            var store = new FasterKV<Key, Value>(
                size: 1L << 20,
                logSettings: logSettings,
                checkpointSettings: new CheckpointSettings { CheckpointDir = path },
                serializerSettings: null,
                comparer: new Key.Comparer()
                );
            return (store, log, path);
        }

        private async static Task PopulateStore(FasterKV<Key, Value> store)
        {
            // Start session with FASTER
            using var s = store.For(new Functions()).NewSession<Functions>();
            Console.WriteLine($"Writing {numKeys} keys to FASTER", numKeys);

            Stopwatch sw = new();
            sw.Start();
            var prevLap = 0;
            for (int ii = 0; ii < numKeys; ii++)
            {
                // lap is used to illustrate the changing values
                var lap = ii / keyMod;

                if (useCheckpoints && lap != prevLap)
                {
                    await store.TakeFullCheckpointAsync(CheckpointType.FoldOver);
                    prevLap = lap;
                }

                var key = new Key(ii % keyMod);

                var value = new Value(key.key + (lap * numKeys * 100));
                if (useRMW)
                    s.RMW(ref key, ref value, serialNo: lap);
                else
                    s.Upsert(ref key, ref value, serialNo: lap);

                // Illustrate that deleted records can be shown as well (unless overwritten by in-place operations, which are not done here)
                if (lap == deleteLap)
                    s.Delete(ref key, serialNo: lap);
            }
            sw.Stop();
            double numSec = sw.ElapsedMilliseconds / 1000.0;
            Console.WriteLine("Total time to upsert {0} elements: {1:0.000} secs ({2:0.00} inserts/sec)", numKeys, numSec, numKeys / numSec);
        }

        private static void ScanStore(FasterKV<Key, Value> store, int keyValue)
        {
            // Start session with FASTER
            using var session = store.For(new Functions()).NewSession<Functions>();

            Console.WriteLine($"Sync scanning records for key {keyValue}");

            var output = default(Value);
            var input = default(Value);
            var key = new Key(keyValue);
            ReadOptions readOptions = new() { ReadFlags = ReadFlags.DisableReadCache};
            for (int lap = 9; /* tested in loop */; --lap)
            {
                var status = session.Read(ref key, ref input, ref output, ref readOptions, out var recordMetadata, serialNo: maxLap + 1);

                // This will wait for each retrieved record; not recommended for performance-critical code or when retrieving multiple records unless necessary.
                if (status.IsPending)
                {
                    session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    using (completedOutputs)
                    {
                        completedOutputs.Next();
                        recordMetadata = completedOutputs.Current.RecordMetadata;
                        status = completedOutputs.Current.Status;
                    }
                }

                if (!ProcessRecord(store, status, recordMetadata.RecordInfo, lap, ref output))
                    break;
            }
        }

        private static async Task ScanStoreAsync(FasterKV<Key, Value> store, int keyValue, CancellationToken cancellationToken)
        {
            // Start session with FASTER
            using var session = store.For(new Functions()).NewSession<Functions>();

            Console.WriteLine($"Async scanning records for key {keyValue}");

            var input = default(Value);
            var key = new Key(keyValue);
            RecordMetadata recordMetadata = default;
            ReadOptions readOptions = new() { ReadFlags = ReadFlags.DisableReadCache };
            for (int lap = 9; /* tested in loop */; --lap)
            {
                var readAsyncResult = await session.ReadAsync(ref key, ref input, ref readOptions, default, serialNo: maxLap + 1, cancellationToken: cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                var (status, output) = readAsyncResult.Complete(out recordMetadata);
                if (!ProcessRecord(store, status, recordMetadata.RecordInfo, lap, ref output))
                    break;
                readOptions.StartAddress = recordMetadata.RecordInfo.PreviousAddress;
            }
        }

        private static bool ProcessRecord(FasterKV<Key, Value> store, Status status, RecordInfo recordInfo, int lap, ref Value output)
        {
            Debug.Assert(status.Found == !recordInfo.Tombstone);
            Debug.Assert((lap == deleteLap) == recordInfo.Tombstone);
            var value = recordInfo.Tombstone ? "<deleted>" : output.value.ToString();
            Console.WriteLine($"  {value}; PrevAddress: {recordInfo.PreviousAddress}");

            // Check for end of loop
            return recordInfo.PreviousAddress >= store.Log.BeginAddress;
        }
    }
}
