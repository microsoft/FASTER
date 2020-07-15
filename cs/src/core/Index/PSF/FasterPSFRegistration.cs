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
        internal PSFManager<FasterKVProviderData<Key, Value>, long> PSFManager { get; private set; }

        internal void InitializePSFManager() 
            => this.PSFManager = new PSFManager<FasterKVProviderData<Key, Value>, long>();

        #region PSF Registration API
        /// <inheritdoc/>
        public IPSF RegisterPSF<TPSFKey>(PSFRegistrationSettings<TPSFKey> registrationSettings,
                                         FasterKVPSFDefinition<Key, Value, TPSFKey> def)
            where TPSFKey : struct
            => this.PSFManager.RegisterPSF(registrationSettings, def);

        /// <inheritdoc/>
        public IPSF[] RegisterPSF<TPSFKey>(PSFRegistrationSettings<TPSFKey> registrationSettings,
                                           params FasterKVPSFDefinition<Key, Value, TPSFKey>[] defs)
            where TPSFKey : struct
            => this.PSFManager.RegisterPSF(registrationSettings, defs);

        /// <inheritdoc/>
        public IPSF RegisterPSF<TPSFKey>(PSFRegistrationSettings<TPSFKey> registrationSettings,
                                         string psfName, Func<Key, Value, TPSFKey?> psfFunc)
            where TPSFKey : struct
            => this.PSFManager.RegisterPSF(registrationSettings, new FasterKVPSFDefinition<Key, Value, TPSFKey>(psfName, psfFunc));

        /// <inheritdoc/>
        public IPSF[] RegisterPSF<TPSFKey>(PSFRegistrationSettings<TPSFKey> registrationSettings,
                                           params (string, Func<Key, Value, TPSFKey?>)[] psfFuncs)
            where TPSFKey : struct
            => this.PSFManager.RegisterPSF(registrationSettings, psfFuncs.Select(e => new FasterKVPSFDefinition<Key, Value, TPSFKey>(e.Item1, e.Item2)).ToArray());

        /// <inheritdoc/>
        public string[][] GetRegisteredPSFNames() => this.PSFManager.GetRegisteredPSFNames();
        #endregion PSF Registration API
    }
}
