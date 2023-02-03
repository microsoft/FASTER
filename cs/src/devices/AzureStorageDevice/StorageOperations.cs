// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace FASTER.devices
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    public partial class BlobManager
    {
        public async Task PerformWithRetriesAsync(
            SemaphoreSlim semaphore,
            bool requireLease,
            string name,
            string intent,
            string data,
            string target,
            int expectedLatencyBound,
            bool isCritical,
            Func<int, Task<long>> operationAsync,
            Func<Task> readETagAsync = null)
        {
            try
            {
                if (semaphore != null)
                {
                    await semaphore.WaitAsync();
                }

                Stopwatch stopwatch = new Stopwatch();
                int numAttempts = 0;
                bool mustReadETagFirst = false;

                while (true) // retry loop
                {
                    numAttempts++;
                    try
                    {
                        if (requireLease)
                        {
                            // we can re-establish the e-tag here because we check the lease afterwards
                            if (mustReadETagFirst)
                            {
                                await readETagAsync().ConfigureAwait(false);
                                mustReadETagFirst = false;
                            }

                            Interlocked.Increment(ref this.LeaseUsers);
                            await this.ConfirmLeaseIsGoodForAWhileAsync();
                        }

                        this.PartitionErrorHandler.Token.ThrowIfCancellationRequested();

                        this.StorageTracer?.FasterStorageProgress($"storage operation {name} ({intent}) started attempt {numAttempts}; target={target} {data}");

                        stopwatch.Restart();

                        long size = await operationAsync(numAttempts).ConfigureAwait(false);

                        stopwatch.Stop();
                        this.StorageTracer?.FasterStorageProgress($"storage operation {name} ({intent}) succeeded on attempt {numAttempts}; target={target} latencyMs={stopwatch.Elapsed.TotalMilliseconds:F1} {data}");

                        if (stopwatch.ElapsedMilliseconds > expectedLatencyBound)
                        {
                            this.TraceHelper.FasterPerfWarning($"storage operation {name} ({intent}) took {stopwatch.Elapsed.TotalSeconds:F1}s on attempt {numAttempts}, which is excessive; {data}");
                        }

                        this.TraceHelper.FasterAzureStorageAccessCompleted(intent, size, name, target, stopwatch.Elapsed.TotalMilliseconds, numAttempts);

                        return;
                    }
                    catch (Exception e) when (this.PartitionErrorHandler.IsTerminated)
                    {
                        string message = $"storage operation {name} ({intent}) was canceled";
                        this.StorageTracer?.FasterStorageProgress(message);
                        throw new OperationCanceledException(message, e);
                    }
                    catch (Exception e) when (BlobUtils.IsTransientStorageError(e) && numAttempts < BlobManager.MaxRetries)
                    {
                        stopwatch.Stop();

                        if (BlobUtils.IsTimeout(e))
                        {
                            this.TraceHelper.FasterPerfWarning($"storage operation {name} ({intent}) timed out on attempt {numAttempts} after {stopwatch.Elapsed.TotalSeconds:F1}s, retrying now; target={target} {data}");
                        }
                        else
                        {
                            TimeSpan nextRetryIn = BlobManager.GetDelayBetweenRetries(numAttempts);
                            this.HandleStorageError(name, $"storage operation {name} ({intent}) failed transiently on attempt {numAttempts}, retry in {nextRetryIn}s", target, e, false, true);
                            await Task.Delay(nextRetryIn);
                        }
                        continue;
                    }
                    catch (Azure.RequestFailedException ex) when (BlobUtilsV12.PreconditionFailed(ex) && readETagAsync != null)
                    {
                        this.StorageTracer?.FasterStorageProgress($"storage operation {name} ({intent}) failed precondition on attempt {numAttempts}; target={target} latencyMs={stopwatch.Elapsed.TotalMilliseconds:F1} {data}");
                        mustReadETagFirst = true;
                        continue;
                    }
                    catch (Exception exception)
                    {
                        this.HandleStorageError(name, $"storage operation {name} ({intent}) failed on attempt {numAttempts}", target, exception, isCritical, this.PartitionErrorHandler.IsTerminated);
                        throw;
                    }
                    finally
                    {
                        if (requireLease)
                        {
                            Interlocked.Decrement(ref this.LeaseUsers);
                        }
                    }
                }
            }
            finally
            {
                if (semaphore != null)
                {
                    semaphore.Release();
                }
            }
        }

        public void PerformWithRetries(
            bool requireLease,
            string name,
            string intent,
            string data,
            string target,
            int expectedLatencyBound,
            bool isCritical,
            Func<int,(long,bool)> operation)
        {
            Stopwatch stopwatch = new Stopwatch();
            int numAttempts = 0;

            while (true) // retry loop
            {
                numAttempts++;
                try
                {
                    if (requireLease)
                    {
                        Interlocked.Increment(ref this.LeaseUsers);
                        this.ConfirmLeaseIsGoodForAWhile();
                    }

                    this.PartitionErrorHandler.Token.ThrowIfCancellationRequested();

                    this.StorageTracer?.FasterStorageProgress($"storage operation {name} ({intent}) started attempt {numAttempts}; target={target} {data}");
                    stopwatch.Restart();

                    (long size, bool completed) = operation(numAttempts);

                    if (!completed)
                    {
                        continue;
                    }

                    stopwatch.Stop();
                    this.StorageTracer?.FasterStorageProgress($"storage operation {name} ({intent}) succeeded on attempt {numAttempts}; target={target} latencyMs={stopwatch.Elapsed.TotalMilliseconds:F1} size={size} {data} ");

                    this.TraceHelper.FasterAzureStorageAccessCompleted(intent, size, name, target, stopwatch.Elapsed.TotalMilliseconds, numAttempts);

                    if (stopwatch.ElapsedMilliseconds > expectedLatencyBound)
                    {
                        this.TraceHelper.FasterPerfWarning($"storage operation {name} ({intent}) took {stopwatch.Elapsed.TotalSeconds:F1}s on attempt {numAttempts}, which is excessive; {data}");
                    }

                    return;
                }
                catch(Exception e) when (this.PartitionErrorHandler.IsTerminated)
                {
                    string message = $"storage operation {name} ({intent}) was canceled";
                    this.StorageTracer?.FasterStorageProgress(message);
                    throw new OperationCanceledException(message, e);  
                }
                catch (Exception e) when (numAttempts < BlobManager.MaxRetries && BlobUtils.IsTransientStorageError(e))
                {
                    stopwatch.Stop();
                    if (BlobUtils.IsTimeout(e))
                    {
                        this.TraceHelper.FasterPerfWarning($"storage operation {name} ({intent}) timed out on attempt {numAttempts} after {stopwatch.Elapsed.TotalSeconds:F1}s, retrying now; target={target} {data}");
                    }
                    else
                    {
                        TimeSpan nextRetryIn = BlobManager.GetDelayBetweenRetries(numAttempts);
                        this.HandleStorageError(name, $"storage operation {name} ({intent}) failed transiently on attempt {numAttempts}, retry in {nextRetryIn}s", target, e, false, true);
                        Thread.Sleep(nextRetryIn);
                    }
                    continue;
                }
                catch (Azure.RequestFailedException ex) when (BlobUtilsV12.PreconditionFailed(ex))
                {
                    // precondition failed, which indicates we are observing a rare partition race
                    this.HandleStorageError(name, $"storage operation {name} ({intent}) failed precondition on attempt {numAttempts}", target, ex, true, true);
                }
                catch (Exception exception)
                {
                    this.HandleStorageError(name, $"storage operation {name} ({intent}) failed on attempt {numAttempts}", target, exception, isCritical, this.PartitionErrorHandler.IsTerminated);
                    throw;
                }
                finally
                {
                    if (requireLease)
                    {
                        Interlocked.Decrement(ref this.LeaseUsers);
                    }
                }
            }
        }
    }
}
