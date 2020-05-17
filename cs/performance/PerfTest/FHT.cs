// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.IO;
using System.Threading;

namespace FASTER.PerfTest
{
    class FHT<TKey, TValue, TOutput, TFunctions>
        where TKey : new()
        where TValue : new() 
        where TFunctions : IFunctions<TKey, TValue, Input, TOutput, Empty>, new()
    {
        internal IFasterKV<TKey, TValue, Input, TOutput, Empty, TFunctions> Faster { get; set; }

        private readonly long fhtSize;
        private LogFiles logFiles;
        private Checkpoint checkpoint;

        internal FHT(bool usePsf, int sizeShift, bool useObjectLog, TestInputs testInputs,
                     SerializerSettings<TKey, TValue> serializerSettings, VariableLengthStructSettings<TKey, TValue> varLenSettings,
                     IFasterEqualityComparer<TKey> keyComparer)
        {
            this.fhtSize = 1L << sizeShift;
            this.logFiles = new LogFiles(useObjectLog, testInputs);

            var checkpointSettings = testInputs.CheckpointMode == Checkpoint.Mode.None
                ? null
                : new CheckpointSettings
                {
                    CheckPointType = Checkpoint.ToCheckpointType(testInputs.CheckpointMode),
                    CheckpointDir = this.logFiles.CheckpointDir
                };

            this.checkpoint = testInputs.CheckpointMode == Checkpoint.Mode.None
                ? null
                : new Checkpoint(testInputs.CheckpointMs, cancellationToken => this.TakeCheckpoint(cancellationToken));

            this.Faster = usePsf
                ? throw new NotImplementedException("TODO: FasterPSF")
                : new FasterKV<TKey, TValue, Input, TOutput, Empty, TFunctions>(
                                fhtSize, new TFunctions(), this.logFiles.LogSettings,
                                checkpointSettings, serializerSettings, keyComparer, varLenSettings);
            this.checkpoint?.Start();
        }

        internal void TakeCheckpoint(CancellationToken cancellationToken)
        {
            // This is called from a dedicated thread
            var initialized = this.Faster.TakeFullCheckpoint(out _);
            if (!initialized)
                throw new InvalidOperationException("TakeFullCheckpoint failed");
            this.Faster.CompleteCheckpointAsync(cancellationToken).GetAwaiter().GetResult();

            // We don't need the checkpoint and we may run out of disk space.
            Directory.Delete(this.logFiles.CheckpointDir, recursive: true);
        }

        internal long LogTailAddress => this.Faster.Log.TailAddress;

        internal void Close()
        {
            if (!(this.checkpoint is null))
            {
                this.checkpoint.Stop();
                this.checkpoint = null;
            }
            if (!(this.Faster is null))
            {
                this.Faster.Dispose();
                this.Faster = null;
            }
            if (!(this.logFiles is null))
            {
                this.logFiles.Close();
                this.logFiles = null;
            }
        }
    }
}
