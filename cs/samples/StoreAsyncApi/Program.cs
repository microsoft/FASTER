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

            FasterKVSettings<CacheKey, CacheValue> fkvSettings = new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                ObjectLogDevice = objlog,
                KeySerializer = () => new CacheKeySerializer(),
                ValueSerializer = () => new CacheValueSerializer()
            };

            faster = new FasterKV<CacheKey, CacheValue>(fkvSettings);

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
            Random rand = new(id);

            bool batched = true; // whether we batch upserts on session
            bool asyncUpsert = false; // whether we use sync or async upsert calls
            bool waitForCommit = false; // whether we wait for commit after each operation (or batch) on this session
            int batchSize = 100; // batch size

            await Task.Yield();

            var context = new CacheContext();
            var taskBatch = new ValueTask<FasterKV<CacheKey, CacheValue>.UpsertAsyncResult<CacheInput, CacheOutput, CacheContext>>[batchSize];
            long seqNo = 0;

            if (!batched)
            {
                // Single upsert at a time, optionally waiting for commit
                // Needs high parallelism (NumParallelTasks) for perf
                // Separate commit thread performs regular checkpoints
                while (true)
                {
                    try
                    {
                        var key = new CacheKey(rand.Next());
                        var value = new CacheValue(rand.Next());
                        if (asyncUpsert)
                        {
                            var r = await session.UpsertAsync(ref key, ref value, context, seqNo++);
                            while (r.Status.IsPending)
                                r = await r.CompleteAsync();
                        }
                        else
                        {
                            session.Upsert(ref key, ref value, context, seqNo++);
                        }
                        if (waitForCommit)
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
                    if (asyncUpsert)
                    {
                        taskBatch[count % batchSize] = session.UpsertAsync(ref key, ref value, context, seqNo++);
                    }
                    else
                    {
                        session.Upsert(ref key, ref value, context, seqNo++);
                    }
                    if (count++ % batchSize == 0)
                    {
                        if (asyncUpsert)
                        {
                            for (int i = 0; i < batchSize; i++)
                            {
                                var r = await taskBatch[i];
                                while (r.Status.IsPending)
                                    r = await r.CompleteAsync();
                            }
                        }
                        if (waitForCommit)
                            await session.WaitForCommitAsync();
                        Interlocked.Add(ref numOps, batchSize);
                    }
                }
            }
        }

        static void ReportThread()
        {
            long lastTime = 0;
            long lastValue = numOps;

            Stopwatch sw = new();
            sw.Start();

            while (true)
            {
                Thread.Sleep(1000);

                var nowTime = sw.ElapsedMilliseconds;
                var nowValue = numOps;
                Console.WriteLine("Operation Throughput: {0} ops/sec, Tail: {1}",
                    1000.0*(nowValue - lastValue) / (nowTime - lastTime), faster.Log.TailAddress);
                lastValue = nowValue;
                lastTime = nowTime;
            }
        }

        static void CommitThread()
        {
            while (true)
            {
                Thread.Sleep(100);
                faster.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver).GetAwaiter().GetResult();
            }
        }
    }
}
