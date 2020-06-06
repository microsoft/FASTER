// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;

namespace FASTER.core
{
    public sealed partial class ClientSession<Key, Value, Input, Output, Context, Functions> :
                                IDisposable
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        #region PSF calls for Secondary FasterKV
        internal Status PsfInsert(ref Key key, ref Value value, ref Input input, long serialNo)
        {
            // Called on the secondary FasterKV
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextPsfInsert(ref key, ref value, ref input, serialNo, ctx);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        internal Status PsfReadKey(ref Key key, ref PSFReadArgs<Key, Value> psfArgs, long serialNo)
        {
            // Called on the secondary FasterKV
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextPsfReadKey(ref key, ref psfArgs, serialNo, ctx);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        internal Status PsfReadAddress(ref PSFReadArgs<Key, Value> psfArgs, long serialNo)
        {
            // Called on the secondary FasterKV
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextPsfReadAddress(ref psfArgs, serialNo, ctx);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        internal Status PsfUpdate<TProviderData, TRecordId>(ref GroupKeysPair groupKeysPair, ref Value value, ref Input input, long serialNo,
                                                            PSFChangeTracker<TProviderData, TRecordId> changeTracker)
            where TRecordId : struct
        {
            // Called on the secondary FasterKV
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextPsfUpdate(ref groupKeysPair, ref value, ref input, serialNo, ctx, changeTracker);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        internal Status PsfDelete<TProviderData, TRecordId>(ref Key key, ref Value value, ref Input input, long serialNo,
                                                            PSFChangeTracker<TProviderData, TRecordId> changeTracker)
            where TRecordId : struct
        {
            // Called on the secondary FasterKV
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextPsfDelete(ref key, ref value, ref input, serialNo, ctx, changeTracker);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        #endregion PSF calls for Secondary FasterKV

        #region PSF Query API for primary FasterKV

        internal FasterKVProviderData<Key, Value> CreateProviderData(long logicalAddress)
        {
            // Looks up logicalAddress in the primary FasterKV
            var psfArgs = new PSFReadArgs<Key, Value>(new PSFInputPrimaryReadAddress<Key>(logicalAddress),
                                                      new PSFOutputPrimaryReadAddress<Key, Value>(this.fht.hlog));

            // Call this directly here because we are within UnsafeResumeThread. TODO: is that OK with an Iterator?
            var status = fht.ContextPsfReadAddress(ref psfArgs, this.ctx.serialNum + 1, ctx);
            var primaryOutput = psfArgs.Output as IPSFPrimaryOutput<FasterKVProviderData<Key, Value>>;
            return status == Status.OK ? primaryOutput.ProviderData : null;    // TODO check other status
        }

        internal IEnumerable<FasterKVProviderData<Key, Value>> ReturnProviderDatas(IEnumerable<long> logicalAddresses)
        {
            foreach (var logicalAddress in logicalAddresses)
            {
                var providerData = this.CreateProviderData(logicalAddress);
                if (!(providerData is null))
                    yield return providerData;
            }
        }

        /// <summary>
        /// Issue a query on a single <see cref="PSF{TPSFKey, TRecordId}"/> on a single key value.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fht.QueryPSF(sizePsf, Size.Medium)) {...}
        /// </example>
        /// <typeparam name="TPSFKey">The type of the key value to return results for</typeparam>
        /// <param name="psf">The Predicate Subset Function object</param>
        /// <param name="psfKey">The key value to return results for</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<Key, Value>> QueryPSF<TPSFKey>(
                PSF<TPSFKey, long> psf, TPSFKey psfKey, PSFQuerySettings querySettings = null)
            where TPSFKey : struct
        {
            // Called on the secondary FasterKV
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return this.ReturnProviderDatas(this.fht.PSFManager.QueryPSF(psf, psfKey, querySettings));
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Issue a query on a single <see cref="PSF{TPSFKey, TRecordId}"/> on multiple key values.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fht.QueryPSF(sizePsf, new TestPSFKey[] { Size.Medium, Size.Large })) {...}
        /// </example>
        /// <typeparam name="TPSFKey">The type of the key value to return results for</typeparam>
        /// <param name="psf">The Predicate Subset Function object</param>
        /// <param name="psfKeys">A vector of key values to return results for; for example, an OR query on
        ///     a single PSF, or a range query for a PSF that generates keys identifying bins.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<Key, Value>> QueryPSF<TPSFKey>
                (PSF<TPSFKey, long> psf, TPSFKey[] psfKeys, PSFQuerySettings querySettings = null)
            where TPSFKey : struct
        {
            // Called on the secondary FasterKV
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return this.ReturnProviderDatas(this.fht.PSFManager.QueryPSF(psf, psfKeys, querySettings));
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Issue a query on a two <see cref="PSF{TPSFKey, TRecordId}"/>s, each with a single key value.
        /// </summary>
        /// <example>
        /// var providerData in fht.QueryPSF(sizePsf, Size.Medium, colorPsf, Color.Red, (l, r) => l || r))
        /// </example>
        /// <typeparam name="TPSFKey1">The type of the key value for the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key value for the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first Predicate Subset Function object</param>
        /// <param name="psf2">The second Predicate Subset Function object</param>
        /// <param name="psfKey1">The key value to return results from the first <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="psfKey2">The key value to return results from the second <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="matchPredicate">A predicate that takes as parameters 1) whether a candidate record matches
        /// the first PSF, 2) whether the record matches the second PSF, and returns a bool indicating whether the
        /// record should be part of the result set. For example, an AND query would return true iff both input
        /// parameters are true, else false; an OR query would return true if either input parameter is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<Key, Value>> QueryPSF<TPSFKey1, TPSFKey2>(
                    PSF<TPSFKey1, long> psf1, TPSFKey1 psfKey1,
                    PSF<TPSFKey2, long> psf2, TPSFKey2 psfKey2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            // Called on the secondary FasterKV
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return this.ReturnProviderDatas(this.fht.PSFManager.QueryPSF(psf1, psfKey1, psf2, psfKey2,
                                                                             matchPredicate, querySettings));
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Issue a query on a two <see cref="PSF{TPSFKey, TRecordId}"/>s, each with a vector of key values.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fht.QueryPSF(
        ///         sizePsf, new TestPSFKey[] { Size.Medium, Size.Large },
        ///         colorPsf, new TestPSFKey[] { Color.Red, Color.Blue},
        ///         (l, r) => l || r))
        /// </example>
        /// <typeparam name="TPSFKey1">The type of the key value for the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key value for the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first Predicate Subset Function object</param>
        /// <param name="psf2">The secojnd Predicate Subset Function object</param>
        /// <param name="psfKeys1">The key values to return results from the first <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="psfKeys2">The key values to return results from the second <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="matchPredicate">A predicate that takes as parameters 1) whether a candidate record matches
        /// the first PSF, 2) whether the record matches the second PSF, and returns a bool indicating whether the
        /// record should be part of the result set. For example, an AND query would return true iff both input
        /// parameters are true, else false; an OR query would return true if either input parameter is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<Key, Value>> QueryPSF<TPSFKey1, TPSFKey2>(
                    PSF<TPSFKey1, long> psf1, TPSFKey1[] psfKeys1,
                    PSF<TPSFKey2, long> psf2, TPSFKey2[] psfKeys2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            // Called on the secondary FasterKV
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return this.ReturnProviderDatas(this.fht.PSFManager.QueryPSF(psf1, psfKeys1, psf2, psfKeys2,
                                                                             matchPredicate, querySettings));
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Issue a query on one or more <see cref="PSF{TPSFKey, TRecordId}"/>s, each with a vector of key values.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fht.QueryPSF(
        ///         new[] {
        ///             (sizePsf, new TestPSFKey[] { Size.Medium, Size.Large }),
        ///             (colorPsf, new TestPSFKey[] { Color.Red, Color.Blue})},
        ///         ll => ll[0]))
        /// </example>
        /// <typeparam name="TPSFKey">The type of the key value for the <see cref="PSF{TPSFKey, TRecordId}"/> vector</typeparam>
        /// <param name="psfsAndKeys">A vector of <see cref="PSF{TPSFKey, TRecordId}"/>s and associated keys to be queried</param>
        /// <param name="matchPredicate">A predicate that takes as a parameters a boolean vector in parallel with 
        /// the <paramref name="psfsAndKeys"/> vector indicating whether a candidate record matches the corresponding
        /// <see cref="PSF{TPSFKey, TRecordId}"/>, and returns a bool indicating whether the record should be part of
        /// the result set. For example, an AND query would return true iff all elements of the input vector are true,
        /// else false; an OR query would return true if element of the input vector is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<Key, Value>> QueryPSF<TPSFKey>(
                    (PSF<TPSFKey, long> psf, TPSFKey[] keys)[] psfsAndKeys,
                    Func<bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey : struct
        {
            // Called on the secondary FasterKV
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return this.ReturnProviderDatas(this.fht.PSFManager.QueryPSF(psfsAndKeys, matchPredicate, querySettings));
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Issue a query on multiple keys <see cref="PSF{TPSFKey, TRecordId}"/>s for two different key types.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fht.QueryPSF(
        ///         new[] {
        ///             (sizePsf, new TestPSFKey[] { Size.Medium, Size.Large }),
        ///             (colorPsf, new TestPSFKey[] { Color.Red, Color.Blue})},
        ///         new[] {(sizePsf, new TestPSFKey[] { Size.Medium, Size.Large }),
        ///             (colorPsf, new TestPSFKey[] { Color.Red, Color.Blue })},
        ///         (ll, rr) => ll[0] || rr[0]))
        /// </example>
        /// <typeparam name="TPSFKey1">The type of the key value for the first vector's <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPSFKey2">The type of the key value for the second vector's <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <param name="psfsAndKeys1">A vector of <see cref="PSF{TPSFKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPSFKey1"/> to be queried</param>
        /// <param name="psfsAndKeys2">A vector of <see cref="PSF{TPSFKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPSFKey2"/> to be queried</param>
        /// <param name="matchPredicate">A predicate that takes as a parameters a boolean vector in parallel with 
        /// the <paramref name="psfsAndKeys1"/> vector and a second boolean vector in parallel with 
        /// the <paramref name="psfsAndKeys2"/> vector, and returns a bool indicating whether the record should be part of
        /// the result set. For example, an AND query would return true iff all elements of both input vectors are true,
        /// else false; an OR query would return true if any element of either input vector is true; and more complex
        /// logic could be done depending on the specific PSFs.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<Key, Value>> QueryPSF<TPSFKey1, TPSFKey2>(
                    (PSF<TPSFKey1, long> psf1, TPSFKey1[] keys1)[] psfsAndKeys1,
                    (PSF<TPSFKey2, long> psf2, TPSFKey2[] keys2)[] psfsAndKeys2,
                    Func<bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            // Called on the secondary FasterKV
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return this.ReturnProviderDatas(this.fht.PSFManager.QueryPSF(psfsAndKeys1, psfsAndKeys2,
                                                                             matchPredicate, querySettings));
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }
        #endregion PSF Query API for primary FasterKV
    }
}
