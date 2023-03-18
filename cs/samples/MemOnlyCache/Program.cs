// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;

#pragma warning disable IDE0079 // Remove unnecessary suppression
#pragma warning disable CS0162 // Unreachable code detected

namespace MemOnlyCache
{
    class Program
    {
        /// <summary>
        /// Maximum number of keys in the database
        /// </summary>
        static int MaxKeys = 10_000_000;
        const string MaxKeyArg = "--maxkeys";

        /// <summary>
        /// Max key size; we choose actual size randomly
        /// </summary>
        static int MaxKeySize = 100;
        const string MaxKeySizeArg = "--keysize";

        /// <summary>
        /// Max value size; we choose actual size randomly
        /// </summary>
        static int MaxValueSize = 1000;
        const string MaxValueSizeArg = "--valuesize";

        /// <summary>
        /// Total in-memory size bits
        /// </summary>
        static int MemorySizeBits = 25; // (2^25 / 24) = ~1.39M key-value pairs (log uses 24 bytes per KV pair)
        const string MemorySizeBitsArg = "--memsizebits";

        /// <summary>
        /// Page size bits
        /// </summary>
        static int PageSizeBits = 14;   // Each page is sized at 2^14 bytes
        const string PageSizeBitsArg = "--pagesizebits";

        /// <summary>
        /// Hashtable size bits
        /// </summary>
        static int HashSizeBits = 20;   // Default is 'no collisions'
        const string HashSizeBitsArg = "--hashsizebits";

        /// <summary>
        /// Number of threads accessing FASTER instances
        /// </summary>
        static int NumThreads = 1;
        const string NumThreadsArg = "-t";

        /// <summary>
        /// Percentage of Reads in incoming workload requests (remaining is updates)
        /// </summary>
        static int ReadPercent = 100;

        /// <summary>
        /// Percentage of RMWs in incoming workload requests
        /// </summary>
        static int RmwPercent = 0;

        /// <summary>
        /// Percentage of Upserts in incoming workload requests
        /// </summary>
        static int UpsertPercent = 0;

        /// <summary>
        /// Percentage of Deletes in incoming workload requests
        /// </summary>
        static int DeletePercent = 0;

        const string OpPercentArg = "--op-rmud%";

        /// <summary>
        /// Uniform random distribution (true) or Zipf distribution (false) of requests
        /// </summary>
        static bool UseUniform = false;
        const string UseUniformArg = "-u";

        /// <summary>
        /// If true, create a log file in the {tempdir}\MemOnlyCacheSample
        /// </summary>
        static bool UseLogFile = false;
        const string UseLogFileArg = "-l";

        /// <summary>
        /// Quiet; do not ask for ENTER to end
        /// </summary>
        static bool Quiet = false;
        const string QuietArg = "-q";

        /// <summary>
        /// Uniform random distribution (true) or Zipf distribution (false) of requests
        /// </summary>
        static bool UseReadCTT = true;
        const string NoReadCTTArg = "--noreadctt";

        /// <summary>
        /// Uniform random distribution (true) or Zipf distribution (false) of requests
        /// </summary>
        static bool UseReadCache = false;
        const string UseReadCacheArg = "--readcache";

        /// <summary>
        /// Percentage of Cache-miss RMWs
        /// </summary>
        static int CacheMissRmwPercent = 0;

        /// <summary>
        /// Percentage of Cache-miss Upserts
        /// </summary>
        static int CacheMissUpsertPercent = 100;
        const string CacheMissInsertPercentArg = "--cm-mu%";

        /// <summary>
        /// Number of seconds to run
        /// </summary>
        static int RunTime = 0;
        const string RunTimeArg = "--runtime";

        /// <summary>
        /// Skew factor (theta) of Zipf distribution
        /// </summary>
        const double Theta = 0.99;

        static FasterKV<CacheKey, CacheValue> h;
        static CacheSizeTracker sizeTracker;
        static long totalReads = 0;
        static long targetSize = 1L << 30; // target size for FASTER, we vary this during the run

        // Cache hit stats
        static long statusNotFound = 0;
        static long statusFound = 0;

        const string HelpArg1 = "-?";
        const string HelpArg2 = "/?";
        const string HelpArg3 = "--help";
        static bool IsHelpArg(string arg) => arg == HelpArg1 || arg == HelpArg2 || arg == HelpArg3;

        static bool done;

        private static bool Usage()
        {
            Console.WriteLine("Runs a loop that illustrates an in-memory cache with dynamic size limit");
            Console.WriteLine("Usage:");
            Console.WriteLine($"  {MaxKeyArg} #: Maximum number of keys in the database. Default = {MaxKeys}");
            Console.WriteLine($"  {MaxKeySizeArg} #: Max key size; we choose actual size randomly. Default = {MaxKeySize}");
            Console.WriteLine($"  {MaxValueSizeArg} #: Max value size; we choose actual size randomly. Default = {MaxValueSize}");
            Console.WriteLine($"  {MemorySizeBitsArg} #: In-memory size of the log, in bits. Default = {MemorySizeBits}");
            Console.WriteLine($"  {PageSizeBitsArg} #: Page size, in bits. Default = {PageSizeBits}");
            Console.WriteLine($"  {HashSizeBitsArg} #: Number of bits in the hash table (recordSize is 24, so '{nameof(MemorySizeBitsArg)}' - '{nameof(HashSizeBitsArg)}' - 5, if positive, is a rough log2 of average tag chain length). Default = {HashSizeBits}");
            Console.WriteLine($"  {OpPercentArg} #,#,#,#: Percentage of [(r)eads,r(m)ws,(u)pserts,(d)eletes] (summing to 0 or 100) operations in incoming workload requests. Default = {ReadPercent},{RmwPercent},{UpsertPercent},{DeletePercent}");
            Console.WriteLine($"  {NoReadCTTArg}: Turn off (true) or allow (false) copying of reads from the Immutable region of the log to the tail of log. Default = {!UseReadCTT}");
            Console.WriteLine($"  {UseReadCacheArg}: Whether to use the ReadCache. Default = {UseReadCache}");
            Console.WriteLine($"  {CacheMissInsertPercentArg} #,#: Whether to insert the key on a cache miss, and if so, the percentage of [r(m)ws,(u)pserts] (summing to 0 or 100) operations to do so. Default = {CacheMissRmwPercent},{CacheMissUpsertPercent}");
            Console.WriteLine($"  {RunTimeArg} #: If nonzero, limits the run to this many seconds. Default = {RunTime}");
            Console.WriteLine($"  {NumThreadsArg} #: Number of threads accessing FASTER instances. Default = {NumThreads}");
            Console.WriteLine($"  {UseUniformArg}: Uniform random distribution (true) or Zipf distribution (false) of requests. Default = {UseUniform}");
            Console.WriteLine($"  {UseLogFileArg}: Use log file (true; written to '{GetLogPath()}') instead of NullDevice (false). Default = {UseLogFile}");
            Console.WriteLine($"  {QuietArg}: Quiet; do not ask for ENTER to end. Default = {Quiet}");
            Console.WriteLine($"  {HelpArg1}, {HelpArg2}, {HelpArg3}: This screen.");
            return false;
        }

        static bool GetArgs(string[] args)
        {
            string arg = string.Empty, val = string.Empty;
            try
            {
                for (var ii = 0; ii < args.Length; ++ii)
                {
                    arg = args[ii].ToLower();
                    val = "n/a";
                    if (IsHelpArg(arg))
                        return Usage();

                    // Flag args (no value)
                    if (arg == UseUniformArg)
                    {
                        UseUniform = true;
                        continue;
                    }
                    if (arg == NoReadCTTArg)
                    {
                        UseReadCTT = false;
                        continue;
                    }
                    if (arg == UseReadCacheArg)
                    {
                        UseReadCache = true;
                        continue;
                    }
                    if (arg == UseLogFileArg)
                    {
                        UseLogFile = true;
                        continue;
                    }
                    if (arg == QuietArg)
                    {
                        Quiet = true;
                        continue;
                    }

                    // Args taking a value
                    if (ii >= args.Length - 1)
                    {
                        Console.WriteLine($"Error: End of arg list encountered while processing arg {arg}; either an unknown flag or a missing value");
                        return false;
                    }
                    val = args[++ii];
                    if (arg == MaxKeyArg)
                    {
                        MaxKeys = int.Parse(val);
                        continue;
                    }
                    if (arg == MaxKeySizeArg)
                    {
                        MaxKeySize = int.Parse(val);
                        continue;
                    }
                    if (arg == MemorySizeBitsArg)
                    {
                        MemorySizeBits = int.Parse(val);
                        continue;
                    }
                    if (arg == PageSizeBitsArg)
                    {
                        PageSizeBits = int.Parse(val);
                        continue;
                    }
                    if (arg == HashSizeBitsArg)
                    {
                        HashSizeBits = int.Parse(val);
                        continue;
                    }
                    if (arg == NumThreadsArg)
                    {
                        NumThreads = int.Parse(val);
                        continue;
                    }
                    if (arg == RunTimeArg)
                    {
                        RunTime = int.Parse(val);
                        continue;
                    }
                    if (arg == OpPercentArg)
                    {
                        var percents = val.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                        var success = percents.Length == 4;
                        if (success)
                        {
                            ReadPercent = int.Parse(percents[0]);
                            RmwPercent = int.Parse(percents[1]);
                            UpsertPercent = int.Parse(percents[2]);
                            DeletePercent = int.Parse(percents[3]);
                            var total = ReadPercent + RmwPercent + UpsertPercent + DeletePercent;
                            success = total == 0 || total == 100;
                        }
                        if (!success)
                        {
                            Console.WriteLine($"{arg} requires 4 values summing to 0 or 100: Percentage of [(r)eads,r(m)ws,(u)pserts,(d)eletes]");
                            return false;
                        }
                        continue;
                    }
                    if (arg == CacheMissInsertPercentArg)
                    {
                        var percents = val.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                        var success = percents.Length == 2;
                        if (success)
                        {
                            CacheMissRmwPercent = int.Parse(percents[0]);
                            CacheMissUpsertPercent = int.Parse(percents[1]);
                            var total = CacheMissRmwPercent + CacheMissUpsertPercent;
                            success = total == 0 || total == 100;
                        }
                        if (!success)
                        {
                            Console.WriteLine($"{arg} requires 2 values summing to 0 or 100: Percentage of [r(m)ws,(u)pserts]");
                            return false;
                        }
                        continue;
                    }

                    Console.WriteLine($"Unknown option: {arg}");
                    return Usage();
                }

                // Note: Here we could verify parameter values and compatibility
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: Arg {arg}, value {val} encountered exception: {ex.Message}");
                return false;
            }
            return true;
        }

        static string GetLogPath() => Path.GetTempPath() + "MemOnlyCacheSample\\";

        static void Main(string[] args)
        {
            // This sample shows the use of FASTER as a concurrent pure in-memory cache
            if (!GetArgs(args))
                return;

            IDevice log, objectLog;
            SerializerSettings<CacheKey, CacheValue> serializerSettings = null;
            if (UseLogFile)
            {
                var path = GetLogPath();
                log = Devices.CreateLogDevice(path + "hlog.log");
                objectLog = Devices.CreateLogDevice(path + "hlog_obj.log");

                serializerSettings = new SerializerSettings<CacheKey, CacheValue>
                {
                    keySerializer = () => new CacheKeySerializer(),
                    valueSerializer = () => new CacheValueSerializer()
                };
            }
            else
            {
                // no storage involved
                log = new NullDevice();
                objectLog = log;
            }

            // Define settings for log
            var logSettings = new LogSettings
            {
                LogDevice = log, ObjectLogDevice = objectLog,
                MutableFraction = 0.9, // 10% of memory log is "read-only region"
                ReadFlags = UseReadCTT ? ReadFlags.CopyReadsToTail : ReadFlags.None, // whether reads in read-only region are copied to tail
                PageSizeBits = PageSizeBits,
                MemorySizeBits = MemorySizeBits
            };

            if (UseReadCache)
                logSettings.ReadCacheSettings = new() { MemorySizeBits = logSettings.MemorySizeBits, PageSizeBits = logSettings.PageSizeBits };

            // Number of records in memory, assuming class keys and values and x64 platform
            // (8-byte key + 8-byte value + 8-byte header = 24 bytes per record)
            const int recordSize = 24;
            int numRecords = (int)(Math.Pow(2, logSettings.MemorySizeBits) / recordSize);

            h = new FasterKV<CacheKey, CacheValue>(1L << HashSizeBits, logSettings, serializerSettings: serializerSettings, comparer: new CacheKey());
            sizeTracker = new CacheSizeTracker(h, targetSize);

            // Initially populate store
            PopulateStore(numRecords);

            // Run continuous read/upsert workload
            ContinuousRandomWorkload();
            
            h.Dispose();
            log.Dispose();
            objectLog.Dispose();

            if (Quiet)
            {
                Console.WriteLine($"Completed RunTime of {RunTime} seconds; exiting");
            }
            else
            { 
                Console.WriteLine("Press <ENTER> to end");
                Console.ReadLine();
            }
        }

        private static void PopulateStore(int count)
        {
            using var s = h.For(new CacheFunctions(sizeTracker)).NewSession<CacheFunctions>();

            Random r = new(0);
            Console.WriteLine("Writing random keys to fill cache");

            for (int i = 0; i < count; i++)
            {
                int k = r.Next(MaxKeys);
                var key = new CacheKey(k, 1 + r.Next(MaxKeySize - 1));
                var value = new CacheValue(1 + r.Next(MaxValueSize - 1), (byte)key.key);
                s.Upsert(ref key, ref value);
            }
        }

        private static void ContinuousRandomWorkload()
        {
            var threads = new Thread[NumThreads];
            for (int i = 0; i < NumThreads; i++)
            {
                var x = i;
                threads[i] = new Thread(() => RandomWorkload(x));
            }
            for (int i = 0; i < NumThreads; i++)
                threads[i].Start();

            Stopwatch sw = new();
            sw.Start();
            var _lastReads = totalReads;
            var _lastTimeMs = sw.ElapsedMilliseconds;
            int count = 0;
            while (RunTime == 0 || (_lastTimeMs / 1000) < RunTime)
            {
                Thread.Sleep(1500);
                var currentReads = totalReads;
                var currentTimeMs = sw.ElapsedMilliseconds;
                var currentElapsed = currentTimeMs - _lastTimeMs;
                var ts = TimeSpan.FromSeconds(currentTimeMs / 1000);
                var totalElapsed = ts.ToString();

                Console.WriteLine("Throughput: {0,8:0.00}K ops/sec; Hit rate: {1:N2}; Memory footprint: {2,12:N2}KB, elapsed: {3:c}", 
                                (currentReads - _lastReads) / (double)(currentElapsed), statusFound / (double)(statusFound + statusNotFound),
                                sizeTracker.TotalSizeBytes / 1024.0, totalElapsed);

                Interlocked.Exchange(ref statusFound, 0);
                Interlocked.Exchange(ref statusNotFound, 0);

                // Optional: perform GC collection to verify accurate memory reporting in task manager
                // GC.Collect();
                // GC.WaitForFullGCComplete();

                // As demo, reduce target size by 100MB each time we report memory footprint
                count++;
                if (count % 4 == 0)
                {
                    if (targetSize > 1L << 28)
                    {
                        targetSize -= 1L << 27;
                        sizeTracker.SetTargetSizeBytes(targetSize);
                    }
                    else
                    {
                        targetSize = 1L << 30;
                        sizeTracker.SetTargetSizeBytes(targetSize);
                    }
                    Console.WriteLine("**** Setting target memory: {0,11:N2}KB", targetSize / 1024.0);
                }

                _lastReads = currentReads;
                _lastTimeMs = currentTimeMs;
            }

            done = true;
            for (int i = 0; i < NumThreads; i++)
                threads[i].Join();
        }

        private static void RandomWorkload(int threadid)
        {
            Console.WriteLine("Issuing {0} random read workload of {1} reads from thread {2}", UseUniform ? "uniform" : "zipf", MaxKeys, threadid);

            using var session = h.For(new CacheFunctions(sizeTracker)).NewSession<CacheFunctions>();

            var rng = new Random(threadid);
            var zipf = new ZipfGenerator(rng, MaxKeys, Theta);

            CacheValue output = default;
            int localStatusFound = 0, localStatusNotFound = 0;

            int i = 0;
            while (!done)
            {
                if ((i % 256 == 0) && (i > 0))
                {
                    Interlocked.Add(ref statusFound, localStatusFound);
                    Interlocked.Add(ref statusNotFound, localStatusNotFound);
                    Interlocked.Add(ref totalReads, 256);
                    localStatusFound = localStatusNotFound = 0;
                }

                var wantValue = RmwPercent + UpsertPercent > 0;

                int op = ReadPercent < 100 ? rng.Next(100) : 99;    // rng.Next() is not inclusive of the upper bound
                long k = UseUniform ? rng.Next(MaxKeys) : zipf.Next();

                var key = new CacheKey(k, 1 + rng.Next(MaxKeySize - 1));

                CacheValue createValue() => new CacheValue(1 + rng.Next(MaxValueSize - 1), (byte)key.key);
                CacheValue value = wantValue ? createValue() : null;

                if (op < ReadPercent)
                {
                    var status = session.Read(ref key, ref output);
                    if (status.IsPending)
                        (status, output) = GetSinglePendingResult(session);
                    if (!status.Found)
                    {
                        if (status.IsFaulted)
                            throw new Exception("Unexpected Error!");
                        localStatusNotFound++;
                        if (CacheMissRmwPercent + CacheMissUpsertPercent > 0)
                        {
                            value ??= createValue();
                            var which = rng.Next(100);
                            if (which < CacheMissRmwPercent)
                            {
                                status = session.RMW(ref key, ref value);
                                if (status.IsPending)
                                    session.CompletePending(wait: true);
                            }
                            else
                                session.Upsert(ref key, ref value);
                        }
                    }
                    else
                    {
                        localStatusFound++;
                        if (output.value[0] != (byte)key.key)
                            throw new Exception("Read value error!");
                    }
                }
                else if (op < ReadPercent + RmwPercent)
                {
                    var status = session.RMW(ref key, ref value);
                    if (status.IsPending)
                        session.CompletePending(wait: true);
                }
                else if (op < ReadPercent + RmwPercent + UpsertPercent)
                {
                    session.Upsert(ref key, ref value);
                }
                else
                {
                    session.Delete(ref key);
                }
                i++;
            }
        }

        internal static (Status status, CacheValue output) GetSinglePendingResult(ClientSession<CacheKey, CacheValue, CacheValue, CacheValue, Empty, CacheFunctions> session)
        {
            session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            if (!completedOutputs.Next())
                throw new Exception("Expected to read one result");
            var result = (completedOutputs.Current.Status, completedOutputs.Current.Output);
            if (completedOutputs.Next())
                throw new Exception("Did not expect to read a second result");
            completedOutputs.Dispose();
            return result;
        }
    }
}
