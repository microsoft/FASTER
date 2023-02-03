// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Lease timing requires better reliability that what we get from asynchronous Task.Delay, 
    /// so we implement a timer wheel.
    /// </summary>
    class LeaseTimer
    {
        const int MaxDelay = 60;
        const int TicksPerSecond = 4;

        static readonly Lazy<LeaseTimer> instance = new Lazy<LeaseTimer>(() => new LeaseTimer());

        readonly Timer timer;
        readonly Entry[] schedule = new Entry[MaxDelay * TicksPerSecond];
        public readonly object reentrancyLock = new object();

        readonly Stopwatch stopwatch = new Stopwatch();
        int performedSteps;

        int position = 0;
        
        public static LeaseTimer Instance => instance.Value;

        public Action<int> DelayWarning { get; set; }

        class Entry
        {
            public TaskCompletionSource<bool> Tcs;
            public CancellationTokenRegistration Registration;
            public Func<Task> Callback;
            public Entry Next;

            public async Task Run()
            {
                try
                {
                    await this.Callback().ConfigureAwait(false);
                    this.Tcs.TrySetResult(true);

                }
                catch (Exception exception) when (!Utils.IsFatal(exception))
                {
                    this.Tcs.TrySetException(exception);
                }
                this.Registration.Dispose();
            }

            public void Cancel()
            {
                this.Tcs.TrySetCanceled();
                this.Registration.Dispose();
            }

            public void RunAll()
            {
                var _ = this.Run();
                if (this.Next != null)
                {
                    this.Next.RunAll();
                }
            }
        }

        LeaseTimer()
        {
            this.timer = new Timer(this.Run, null, 0, 1000 / TicksPerSecond);
            this.stopwatch.Start();
        }

        public void Run(object _)
        {
            if (Monitor.TryEnter(this.reentrancyLock))
            {
                try
                {
                    var stepsToDo = (this.stopwatch.ElapsedMilliseconds * TicksPerSecond / 1000) - this.performedSteps;

                    if (stepsToDo > 5 * TicksPerSecond)
                    {
                        this.DelayWarning?.Invoke((int)stepsToDo / TicksPerSecond);
                    }

                    for (int i = 0; i < stepsToDo; i++)
                    {
                        this.AdvancePosition();
                    }
                }
                finally
                {
                    Monitor.Exit(this.reentrancyLock);
                }
            }
        }

        void AdvancePosition()
        {
            int position = this.position;
            this.position = (position + 1) % (MaxDelay * TicksPerSecond);
            this.performedSteps++;

            Entry current;
            while (true)
            {
                current = this.schedule[position];
                if (current == null || Interlocked.CompareExchange<Entry>(ref this.schedule[position], null, current) == current)
                {
                    break;
                }
            }

            current?.RunAll();
        }

        public Task Schedule(int delay, Func<Task> callback, CancellationToken token)
        {
            if ((delay / 1000) >= MaxDelay || delay < 0)
            {
                throw new ArgumentException(nameof(delay));
            }

            var entry = new Entry()
            {
               Tcs = new TaskCompletionSource<bool>(),
               Callback = callback,
            };

            entry.Registration = token.Register(entry.Cancel);

            while (true)
            {
                int targetPosition = (this.position + (delay * TicksPerSecond) / 1000) % (MaxDelay * TicksPerSecond);

                if (targetPosition == this.position)
                {
                    return callback();
                }
                else
                {
                    Entry current = this.schedule[targetPosition];
                    entry.Next = current;
                    if (Interlocked.CompareExchange<Entry>(ref this.schedule[targetPosition], entry, current) == current)
                    {
                        return entry.Tcs.Task;
                    }
                }
            }
        }
    }
}
