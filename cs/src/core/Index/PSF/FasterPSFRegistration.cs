// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Linq;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.core
{
    // PSF-related functions for FasterKV
    public partial class FasterKV<Key, Value, Input, Output, Context, Functions>
        : FasterBase, IFasterKV<Key, Value, Input, Output, Context, Functions>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        // Some FasterKV ctor params we need to pass through.
        private readonly long hashTableSize;
        private readonly LogSettings logSettings;
        private readonly CheckpointSettings checkpointSettings;
        private readonly SerializerSettings<Key, Value> serializerSettings;
        private readonly VariableLengthStructSettings<Key, Value> variableLengthStructSettings;

        internal PSFManager<FasterKVProviderData<Key, Value>, long> PSFManager { get; private set; }

        internal void InitializePSFs() 
            => this.PSFManager = new PSFManager<FasterKVProviderData<Key, Value>, long>();

        private PSFRegistrationSettings<TPSFKey> CreateDefaultRegistrationSettings<TPSFKey>() 
            => new PSFRegistrationSettings<TPSFKey>
                {
                    HashTableSize = this.hashTableSize,
                    LogSettings = this.logSettings,
                    CheckpointSettings = this.checkpointSettings // TODO fix this up
                };

        #region PSF Registration API
        /// <inheritdoc/>
        public PSF<TPSFKey, long> RegisterPSF<TPSFKey>(
                FasterKVPSFDefinition<Key, Value, TPSFKey> def,
                PSFRegistrationSettings<TPSFKey> registrationSettings = null)
            where TPSFKey : struct
            => this.PSFManager.RegisterPSF(def, registrationSettings ?? CreateDefaultRegistrationSettings<TPSFKey>());

        /// <inheritdoc/>
        public PSF<TPSFKey, long>[] RegisterPSF<TPSFKey>
                (FasterKVPSFDefinition<Key, Value, TPSFKey>[] defs,
                PSFRegistrationSettings<TPSFKey> registrationSettings = null)
            where TPSFKey : struct
            => this.PSFManager.RegisterPSF(defs, registrationSettings ?? CreateDefaultRegistrationSettings<TPSFKey>());

        /// <inheritdoc/>
        public PSF<TPSFKey, long> RegisterPSF<TPSFKey>(
                string psfName, Func<Key, Value, TPSFKey?> psfFunc,
                PSFRegistrationSettings<TPSFKey> registrationSettings = null)
            where TPSFKey : struct
            => this.PSFManager.RegisterPSF(new FasterKVPSFDefinition<Key, Value, TPSFKey>(psfName, psfFunc),
                                           registrationSettings ?? CreateDefaultRegistrationSettings<TPSFKey>());

        /// <inheritdoc/>
        public PSF<TPSFKey, long>[] RegisterPSF<TPSFKey>(
                params (string, Func<Key, Value, TPSFKey?>)[] psfFuncs)
            where TPSFKey : struct
            => this.PSFManager.RegisterPSF(psfFuncs.Select(e => new FasterKVPSFDefinition<Key, Value, TPSFKey>(e.Item1, e.Item2)).ToArray(),
                                           CreateDefaultRegistrationSettings<TPSFKey>());

        /// <inheritdoc/>
        public PSF<TPSFKey, long>[] RegisterPSF<TPSFKey>(
                (string, Func<Key, Value, TPSFKey?>)[] psfDefs,
                PSFRegistrationSettings<TPSFKey> registrationSettings = null)
            where TPSFKey : struct
            => this.PSFManager.RegisterPSF(psfDefs.Select(e => new FasterKVPSFDefinition<Key, Value, TPSFKey>(e.Item1, e.Item2)).ToArray(),
                                           CreateDefaultRegistrationSettings<TPSFKey>());

        /// <inheritdoc/>
        public string[][] GetRegisteredPSFs() => this.PSFManager.GetRegisteredPSFs();
        #endregion PSF Registration API
    }
}
