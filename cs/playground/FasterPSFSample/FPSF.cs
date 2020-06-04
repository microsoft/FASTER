// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FasterPSFSample
{
    class FPSF<TValue, TOutput, TFunctions, TSerializer>
        where TValue : IOrders, new()
        where TOutput : new()
        where TFunctions : IFunctions<Key, TValue, Input, TOutput, Context<TValue>>, new()
        where TSerializer : BinaryObjectSerializer<TValue>, new()
    {
        internal IFasterKV<Key, TValue, Input, TOutput, Context<TValue>, TFunctions> fht { get; set; }

        private LogFiles logFiles;

        internal PSF<SizeKey, long> SizePsf;
        internal PSF<ColorKey, long> ColorPsf;

        internal FPSF(bool useObjectValues, bool useReadCache)
        {
            this.logFiles = new LogFiles(useObjectValues, useReadCache);

            this.fht = new FasterKV<Key, TValue, Input, TOutput, Context<TValue>, TFunctions>(
                                1L << 20, new TFunctions(), this.logFiles.LogSettings,
                                null, // TODO: add checkpoints
                                useObjectValues
                                ? new SerializerSettings<Key, TValue> { valueSerializer = () => new TSerializer() } : null);

            this.SizePsf = fht.RegisterPSF("sizePsf", (k, v) => new SizeKey((Constants.Size)v.SizeInt));
            this.ColorPsf = fht.RegisterPSF("colorPsf", (k, v) => new ColorKey(Constants.ColorDict[v.ColorArgb]));
        }

        internal void Close()
        {
            if (!(this.fht is null))
            {
                this.fht.Dispose();
                this.fht = null;
            }
            if (!(this.logFiles is null))
            {
                this.logFiles.Close();
                this.logFiles = null;
            }
        }
    }
}
