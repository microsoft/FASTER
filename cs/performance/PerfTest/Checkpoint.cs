// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.PerfTest
{
    public class Checkpoint
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public enum Mode
        {
            None,
            Snapshot,
            FoldOver
        }

        private readonly int ms;
        readonly Action callback;
        readonly CancellationTokenSource cts;

        internal Checkpoint(int ms, Action callback)
        {
            this.ms = ms;
            this.callback = callback;
            this.cts = new CancellationTokenSource();
        }

        internal static CheckpointType ToCheckpointType(Mode mode)
            => mode switch
            {
                Mode.Snapshot => CheckpointType.Snapshot,
                Mode.FoldOver => CheckpointType.FoldOver,
                _ => throw new InvalidOperationException($"Unexpected mode {mode}")
            };

        internal void Start()
        {
            Task.Run(() => this.Run());
        }

        private async void Run()
        {
            if (Globals.Verbose)
                Console.WriteLine($"Checkpointing started every {this.ms}ms...");
            try
            {
                while (!this.cts.Token.IsCancellationRequested)
                {
                    await Task.Delay(ms, this.cts.Token);
                    if (!this.cts.Token.IsCancellationRequested)
                        this.callback();
                }
            }
            catch (TaskCanceledException)
            {
                // This is how we end the loop
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in Checkpoint thread:");
                Console.WriteLine(ex);
            }
        }

        internal void Stop() => this.cts.Cancel();
    }
}
