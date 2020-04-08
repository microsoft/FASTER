// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace FASTER.PerfTest
{
    class FHT<TValue, TOutput, TFunctions, TSerializer>
        where TValue : new() 
        where TFunctions : IFunctions<Key, TValue, Input, TOutput, Empty>, new()
        where TSerializer : BinaryObjectSerializer<TValue>, new()
    {
        internal IFasterKV<Key, TValue, Input, TOutput, Empty, TFunctions> Faster { get; set; }

        private readonly long fhtSize;

        private LogFiles logFiles;

        internal FHT(bool usePsf, int sizeShift, bool useVarLenValues, bool useObjectValues, bool useReadCache)
        {
            this.fhtSize = 1L << sizeShift;
            this.logFiles = new LogFiles(useObjectValues, useReadCache);

            var varLenSettings = useVarLenValues
                ? new VariableLengthStructSettings<Key, TValue> { valueLength = new VarLenValueLength<TValue>() }
                : null;

            var serializerSettings = useObjectValues
                ? new SerializerSettings<Key, TValue> { valueSerializer = () => new TSerializer() }
                : null;

            this.Faster = usePsf
                ? throw new NotImplementedException("FasterPSF")
                : new FasterKV<Key, TValue, Input, TOutput, Empty, TFunctions>(
                                fhtSize, new TFunctions(), this.logFiles.LogSettings,
                                null, // TODO: add checkpoints
                                serializerSettings, null, varLenSettings);
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
