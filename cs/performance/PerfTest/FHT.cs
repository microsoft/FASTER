// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

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

        internal FHT(bool usePsf, int sizeShift, bool useObjectLog, bool useReadCache,
                     SerializerSettings<TKey, TValue> serializerSettings, VariableLengthStructSettings<TKey, TValue> varLenSettings,
                     IFasterEqualityComparer<TKey> keyComparer, Checkpoint.Mode checkpointMode, int checkpointMs)
        {
            this.fhtSize = 1L << sizeShift;
            this.logFiles = new LogFiles(useObjectLog, useReadCache);

            var checkpointSettings = checkpointMode == Checkpoint.Mode.None
                ? null
                : new CheckpointSettings
                {
                    CheckPointType = Checkpoint.ToCheckpointType(checkpointMode),
                    CheckpointDir = this.logFiles.directory
                };

            this.checkpoint = checkpointMode == Checkpoint.Mode.None
                ? null
                : new Checkpoint(checkpointMs, () => this.TakeCheckpoint());

            this.Faster = usePsf
                ? throw new NotImplementedException("TODO: FasterPSF")
                : new FasterKV<TKey, TValue, Input, TOutput, Empty, TFunctions>(
                                fhtSize, new TFunctions(), this.logFiles.LogSettings,
                                checkpointSettings, serializerSettings, keyComparer, varLenSettings);
            this.checkpoint?.Start();
        }

        internal void TakeCheckpoint()
        {
            // This is called from a dedicated thread
            var initialized = this.Faster.TakeFullCheckpoint(out _);
            if (!initialized)
                throw new InvalidOperationException("TakeFullCheckpoint failed");
            this.Faster.CompleteCheckpointAsync().GetAwaiter().GetResult();
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
