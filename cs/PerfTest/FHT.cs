// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace PerfTest
{
    class FHT<TValue, TOutput, TFunctions, TSerializer>
        where TValue : ICacheValue<TValue>, new() 
        where TFunctions : IFunctions<CacheKey, TValue, CacheInput, TOutput, CacheContext>, new()
        where TSerializer : BinaryObjectSerializer<TValue>, new()
    {
        internal IFasterKV<CacheKey, TValue, CacheInput, TOutput, CacheContext, TFunctions> Faster { get; set; }

        const long FhtSize = 1L << 20;

        private LogFiles logFiles;

        internal FHT(bool usePsf, bool useVarLenValues, bool useObjectValues, bool useReadCache)
        {
            this.logFiles = new LogFiles(useObjectValues, useReadCache);

            var varLenSettings = useVarLenValues
                ? new VariableLengthStructSettings<CacheKey, TValue> { valueLength = new VarLenValueLength<TValue>() }
                : null;

            var serializerSettings = useObjectValues
                ? new SerializerSettings<CacheKey, TValue> { valueSerializer = () => new TSerializer() }
                : null;

            this.Faster = usePsf
                ? throw new NotImplementedException("FasterPSF")
                : new FasterKV<CacheKey, TValue, CacheInput, TOutput, CacheContext, TFunctions>(
                                FhtSize, new TFunctions(), this.logFiles.LogSettings,
                                null, // TODO: add checkpoints
                                serializerSettings, null, varLenSettings);
        }

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
