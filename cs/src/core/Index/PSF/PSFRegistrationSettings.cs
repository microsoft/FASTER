// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Options for PSF registration.
    /// </summary>
    public class PSFRegistrationSettings<TPSFKey>
    {
        /// <summary>
        /// When registring new PSFs over an existing store, this is the logicalAddress in the primary
        /// FasterKV at which indexing will be started.
        /// </summary>
        public long IndexFromAddress = Constants.kInvalidAddress;

        /// <summary>
        /// The hash table size to be used in the PSF-implementing secondary FasterKV instances.
        /// For PSFs defined on a FasterKV instance, if this is 0 or less, it will use the same value
        /// passed to the primary FasterKV instance.
        /// </summary>
        public long HashTableSize = 0;

        /// <summary>
        /// The log settings to be used in the PSF-implementing secondary FasterKV instances.
        /// For PSFs defined on a FasterKV instance, if this is null, it will use the same settings as
        /// passed to the primary FasterKV instance.
        /// </summary>
        public LogSettings LogSettings;

        /// <summary>
        /// The log settings to be used in the PSF-implementing secondary FasterKV instances.
        /// For PSFs defined on a FasterKV instance, if this is null, it will use the same settings
        /// consistent with those passed to the primary FasterKV instance.
        /// </summary>
        public CheckpointSettings CheckpointSettings;

        /// <summary>
        /// Optional key comparer; if null, <typeparamref name="TPSFKey"/> should implement
        ///     <see cref="IFasterEqualityComparer{TPSFKey}"/>; otherwise a slower EqualityComparer will be used.
        /// </summary>
        public IFasterEqualityComparer<TPSFKey> KeyComparer;

        /// <summary>
        /// Indicates whether PSFGroup Sessions are thread-affinitized.
        /// </summary>
        public bool ThreadAffinitized;

        /// <summary>
        /// The size of the first IPU Cache; inserts are done into this cache only. If zero, no caching is done.
        /// </summary>
        public long IPU1CacheSize = 0;

        /// <summary>
        /// The size of the second IPU Cache; inserts are not done into this cache, so more distant records
        /// are likelier to remain. If this is nonzero, <see cref="IPU1CacheSize"/> must also be nonzero.
        /// </summary>
        public long IPU2CacheSize = 0;
    }
}
