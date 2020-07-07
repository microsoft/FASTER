// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FasterPSFSample
{
    class FPSF<TValue, TInput, TOutput, TFunctions, TSerializer>
        where TValue : IOrders, new()
        where TOutput : new()
        where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
        where TSerializer : BinaryObjectSerializer<TValue>, new()
    {
        internal IFasterKV<Key, TValue, TInput, TOutput, Context<TValue>, TFunctions> fht { get; set; }

        private LogFiles logFiles;

        internal PSF<SizeKey, long> SizePsf;
        internal PSF<ColorKey, long> ColorPsf;
        internal PSF<CountBinKey, long> CountBinPsf;

        internal PSF<CombinedKey, long> CombinedSizePsf;
        internal PSF<CombinedKey, long> CombinedColorPsf;
        internal PSF<CombinedKey, long> CombinedCountBinPsf;

        internal FPSF(bool useObjectValues, bool useMultiGroup, bool useReadCache)
        {
            this.logFiles = new LogFiles(useObjectValues, useReadCache, useMultiGroup ? 3 : 1);

            this.fht = new FasterKV<Key, TValue, TInput, TOutput, Context<TValue>, TFunctions>(
                                1L << 20, new TFunctions(), this.logFiles.LogSettings,
                                null, // TODO: add checkpoints
                                useObjectValues ? new SerializerSettings<Key, TValue> { valueSerializer = () => new TSerializer() } : null);

            if (useMultiGroup)
            {
                var psfOrdinal = 0;
                this.SizePsf = fht.RegisterPSF("sizePsf", (k, v) => new SizeKey((Constants.Size)v.SizeInt),
                    CreatePSFRegistrationSettings<SizeKey>(psfOrdinal++));
                this.ColorPsf = fht.RegisterPSF("colorPsf", (k, v) => new ColorKey(Constants.ColorDict[v.ColorArgb]),
                    CreatePSFRegistrationSettings<ColorKey>(psfOrdinal++));
                this.CountBinPsf = fht.RegisterPSF("countBinPsf", (k, v) => CountBinKey.GetBin(v.Count, out int bin)
                                                                    ? new CountBinKey(bin) : (CountBinKey?)null,
                    CreatePSFRegistrationSettings<CountBinKey>(psfOrdinal++));
            }
            else
            {
                var psfs = fht.RegisterPSF(new(string, Func<Key, TValue, CombinedKey?>)[] {
                    ("sizePsf", (k, v) => new CombinedKey((Constants.Size)v.SizeInt)),
                    ("colorPsf", (k, v) => new CombinedKey(Constants.ColorDict[v.ColorArgb])),
                    ("countBinPsf", (k, v) => CountBinKey.GetBin(v.Count, out int bin)
                                                                    ? new CombinedKey(bin) : (CombinedKey?)null)
                    }, CreatePSFRegistrationSettings<CombinedKey>(0)
                );
                this.CombinedSizePsf = psfs[0];
                this.CombinedColorPsf = psfs[1];
                this.CombinedCountBinPsf = psfs[2];
            }
        }

        PSFRegistrationSettings<TKey> CreatePSFRegistrationSettings<TKey>(int psfOrdinal)
        {
            var regSettings = new PSFRegistrationSettings<TKey>
            {
                HashTableSize = 1L << 20,
                LogSettings = this.logFiles.PSFLogSettings[psfOrdinal],
                CheckpointSettings = null,  // TODO checkpoints
                IPU1CacheSize = 0,          // TODO IPUCache
                IPU2CacheSize = 0
            };
            
            // Override some things.
            var regLogSettings = regSettings.LogSettings;
            regLogSettings.PageSizeBits = 20;
            regLogSettings.SegmentSizeBits = 25;
            regLogSettings.MemorySizeBits = 29;
            regLogSettings.CopyReadsToTail = false;    // TODO--test this in both primary and secondary FKV
            if (!(regLogSettings.ReadCacheSettings is null))
            {
                regLogSettings.ReadCacheSettings.PageSizeBits = regLogSettings.PageSizeBits;
                regLogSettings.ReadCacheSettings.MemorySizeBits = regLogSettings.MemorySizeBits;
            }
            return regSettings;
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
