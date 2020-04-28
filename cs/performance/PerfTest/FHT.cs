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

        internal FHT(bool usePsf, int sizeShift, bool useObjectLog, bool useReadCache,
                     SerializerSettings<TKey, TValue> serializerSettings, VariableLengthStructSettings<TKey, TValue> varLenSettings,
                     IFasterEqualityComparer<TKey> keyComparer)
        {
            this.fhtSize = 1L << sizeShift;
            this.logFiles = new LogFiles(useObjectLog, useReadCache);

            this.Faster = usePsf
                ? throw new NotImplementedException("FasterPSF")
                : new FasterKV<TKey, TValue, Input, TOutput, Empty, TFunctions>(
                                fhtSize, new TFunctions(), this.logFiles.LogSettings,
                                null, // TODO: add checkpoints
                                serializerSettings, keyComparer, varLenSettings);
        }

        internal long LogTailAddress => this.Faster.Log.TailAddress;

        internal void Close()
        {
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
