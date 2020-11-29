// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;

namespace StoreAsyncApi
{
    public class Program
    {
        static FasterKV<CacheKey, CacheValue> faster;
        static int numOps = 0;

        /// <summary>
        /// Main program entry point
        /// </summary>
        static void Main()
        {
            var path = Path.GetTempPath() + "StoreAsyncApi/";

            // Since the example runs forever, we set option to auto-delete files on close
            // Note that this setting precludes recovery
            var log = Devices.CreateLogDevice(path + "hlog.log", deleteOnClose: true);
            var objlog = Devices.CreateLogDevice(path + "hlog.obj.log", deleteOnClose: true);

            var logSettings = new LogSettings { LogDevice = log, ObjectLogDevice = objlog };
            var checkpointSettings = new CheckpointSettings { CheckpointDir = path, CheckPointType = CheckpointType.FoldOver };
            var serializerSettings = new SerializerSettings<CacheKey, CacheValue> { keySerializer = () => new CacheKeySerializer(), valueSerializer = () => new CacheValueSerializer() };

            faster = new FasterKV<CacheKey, CacheValue>
                (1L << 20, logSettings, checkpointSettings, serializerSettings);

            const int NumParallelTasks = 1;
            ThreadPool.SetMinThreads(2 * Environment.ProcessorCount, 2 * Environment.ProcessorCount);
            TaskScheduler.UnobservedTaskException += (object sender, UnobservedTaskExceptionEventArgs e) =>
            {
                Console.WriteLine($"Unobserved task exception: {e.Exception}");
                e.SetObserved();
            };

            Task[] tasks = new Task[NumParallelTasks];
            for (int i = 0; i < NumParallelTasks; i++)
            {
                int local = i;
                tasks[i] = Task.Run(() => AsyncOperator(local));
            }

            // Threads for reporting, commit
            new Thread(new ThreadStart(ReportThread)).Start();
            new Thread(new ThreadStart(CommitThread)).Start();

            Task.WaitAll(tasks);
        }

        /// <summary>
        /// Async operations on FasterKV
        /// </summary>
        static async Task AsyncOperator(int id)
        {
            using var session = faster.For(new CacheFunctions()).NewSession<CacheFunctions>(id.ToString());
            Random rand = new Random(id);

            bool batched = true;

            await Task.Yield();

            var context = new CacheContext();

            if (!batched)
            {
                // Single commit version - upsert each item and wait for commit
                // Needs high parallelism (NumParallelTasks) for perf
                // Needs separate commit thread to perform regular checkpoints
                while (true)
                {
                    try
                    {

                        var key = new CacheKey(rand.Next());
                        var value = new CacheValue(rand.Next());
                        session.Upsert(ref key, ref value, context);
                        await session.WaitForCommitAsync();

                        Interlocked.Increment(ref numOps);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"{nameof(AsyncOperator)}({id}): {ex}");
                    }
                }
            }
            else
            {
                // Batched version - we enqueue many entries to memory,
                // then wait for commit periodically
                int count = 0;

                while (true)
                {

                    var key = new CacheKey(rand.Next());
                    var value = new CacheValue(rand.Next());
                    session.Upsert(ref key, ref value, context);

                    if (count++ % 100 == 0)
                    {
                        await session.WaitForCommitAsync();
                        Interlocked.Add(ref numOps, 100);
                    }
                }
            }
        }

        static void ReportThread()
        {
            long lastTime = 0;
            long lastValue = numOps;

            Stopwatch sw = new Stopwatch();
            sw.Start();

            while (true)
            {
                Thread.Sleep(5000);

                var nowTime = sw.ElapsedMilliseconds;
                var nowValue = numOps;

                Console.WriteLine("Operation Throughput: {0} ops/sec, Tail: {1}",
                    (nowValue - lastValue) / (1000 * (nowTime - lastTime)), faster.Log.TailAddress);
                lastValue = nowValue;
                lastTime = nowTime;
            }
        }

        static void CommitThread()
        {
            while (true)
            {
                Thread.Sleep(5000);
                faster.TakeFullCheckpointAsync(CheckpointType.FoldOver).GetAwaiter().GetResult();
            }
        }
    }
}
