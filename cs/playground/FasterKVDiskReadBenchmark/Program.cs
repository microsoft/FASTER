// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;

namespace FasterKVDiskReadBenchmark
{
    public class Program
    {
        static FasterKV<Key, Value, Input, Output, Empty, MyFuncs> faster;
        static int numOps = 0;

        const int NumParallelSessions = 1;
        const int NumKeys = 20_000_000 / NumParallelSessions;
        const bool periodicCommit = false;
        const bool useAsync = false;
        const bool readBatching = true;
        const int readBatchSize = 100;

        /// <summary>
        /// Main program entry point
        /// </summary>
        static void Main()
        {
            var path = "FasterKVDiskReadBenchmark";
            var log = Devices.CreateLogDevice(path + "hlog.log", deleteOnClose: true);

            var logSettings = new LogSettings { LogDevice = log, MemorySizeBits = 25, PageSizeBits = 20 };
            var checkpointSettings = new CheckpointSettings { CheckpointDir = path, CheckPointType = CheckpointType.FoldOver };

            faster = new FasterKV<Key, Value, Input, Output, Empty, MyFuncs>
                (1L << 25, new MyFuncs(), logSettings, checkpointSettings);

            ThreadPool.SetMinThreads(2 * Environment.ProcessorCount, 2 * Environment.ProcessorCount);
            TaskScheduler.UnobservedTaskException += (object sender, UnobservedTaskExceptionEventArgs e) =>
            {
                Console.WriteLine($"Unobserved task exception: {e.Exception}");
                e.SetObserved();
            };

            // Threads for reporting, commit
            new Thread(new ThreadStart(ReportThread)).Start();
            if (periodicCommit)
                new Thread(new ThreadStart(CommitThread)).Start();


            Task[] tasks = new Task[NumParallelSessions];
            for (int i = 0; i < NumParallelSessions; i++)
            {
                int local = i;
                tasks[i] = Task.Run(() => AsyncUpsertOperator(local));
            }
            Task.WaitAll(tasks);

            tasks = new Task[NumParallelSessions];
            for (int i = 0; i < NumParallelSessions; i++)
            {
                int local = i;
                tasks[i] = Task.Run(() => AsyncReadOperator(local));
            }
            Task.WaitAll(tasks);
        }

        /// <summary>
        /// Async upsert operations on FasterKV
        /// </summary>
        static async Task AsyncUpsertOperator(int id)
        {
            using var session = faster.NewSession(id.ToString() + "upsert");
            await Task.Yield();

            try
            {
                Key key;
                Value value;
                for (int i = NumKeys * id; i < NumKeys * (id + 1); i++)
                {
                    key = new Key(i);
                    value = new Value(i, i);
                    if (useAsync)
                        await session.UpsertAsync(ref key, ref value);
                    else
                        session.Upsert(ref key, ref value, Empty.Default, 0);
                    Interlocked.Increment(ref numOps);

                    if (periodicCommit && i % 100 == 0)
                    {
                        await session.WaitForCommitAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{nameof(AsyncUpsertOperator)}({id}): {ex}");
            }
        }

        /// <summary>
        /// Async read operations on FasterKV
        /// </summary>
        static async Task AsyncReadOperator(int id)
        {
            using var session = faster.NewSession(id.ToString() + "read");
            Random rand = new Random(id);

            await Task.Yield();

            try
            {
                Key key;
                Input input = default;
                int i = 0;

                var tasks = new (long, ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Empty, MyFuncs>>)[readBatchSize];
                while (true)
                {
                    key = new Key(NumKeys * id + rand.Next(0, NumKeys));

                    if (useAsync)
                    {
                        if (readBatching)
                        {
                            tasks[i % readBatchSize] = (key.key, session.ReadAsync(ref key, ref input));
                        }
                        else
                        {
                            var result = (await session.ReadAsync(ref key, ref input)).CompleteRead();
                            if (result.Item1 != Status.OK || result.Item2.value.vfield1 != key.key || result.Item2.value.vfield2 != key.key)
                            {
                                throw new Exception("Wrong value found");
                            }
                        }
                    }
                    else
                    {
                        Output output = new Output();
                        var result = session.Read(ref key, ref input, ref output, Empty.Default, 0);
                        if (readBatching)
                        {
                            if (result != Status.PENDING)
                            {
                                if (output.value.vfield1 != key.key || output.value.vfield2 != key.key)
                                {
                                    throw new Exception("Wrong value found");
                                }
                            }
                        }
                        else
                        {
                            if (result == Status.PENDING)
                            {
                                session.CompletePending(true);
                            }
                            if (output.value.vfield1 != key.key || output.value.vfield2 != key.key)
                            {
                                throw new Exception("Wrong value found");
                            }
                        }
                    }

                    Interlocked.Increment(ref numOps);
                    i++;

                    if (readBatching && (i % readBatchSize == 0))
                    {
                        if (useAsync)
                        {
                            for (int j = 0; j < readBatchSize; j++)
                            {
                                var result = (await tasks[j].Item2).CompleteRead();
                                if (result.Item1 != Status.OK || result.Item2.value.vfield1 != tasks[j].Item1 || result.Item2.value.vfield2 != tasks[j].Item1)
                                {
                                    throw new Exception($"Wrong value found. Found: {result.Item2.value.vfield1}, Expected: {tasks[j].Item1}");
                                }
                            }
                        }
                        else
                        {
                            session.CompletePending(true);
                        }
                    }

                    if (periodicCommit && i % 100 == 0)
                    {
                        await session.WaitForCommitAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{nameof(AsyncReadOperator)}({id}): {ex}");
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

                Console.WriteLine("Operation Throughput: {0}K ops/sec, Tail: {1}",
                    (nowValue - lastValue) / (double)(nowTime - lastTime), faster.Log.TailAddress);
                lastValue = nowValue;
                lastTime = nowTime;
            }
        }

        static void CommitThread()
        {
            while (true)
            {
                Thread.Sleep(5000);

                faster.TakeFullCheckpoint(out _);
                faster.CompleteCheckpointAsync().GetAwaiter().GetResult();
            }
        }
    }
}
