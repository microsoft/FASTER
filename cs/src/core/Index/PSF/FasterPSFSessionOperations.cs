// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

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

        internal Status PsfUpdate<TProviderData>(ref GroupKeysPair groupKeysPair, ref Value value, ref Input input, long serialNo,
                                                 PSFChangeTracker<TProviderData, Value> changeTracker)
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

        internal Status PsfDelete<TProviderData>(ref Key key, ref Value value, ref Input input, long serialNo,
                                                 PSFChangeTracker<TProviderData, Value> changeTracker)
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

        internal Status CreateProviderData(long logicalAddress,
                                           ConcurrentQueue<FasterKVProviderData<Key, Value>> providerDatas)
        {
            // Looks up logicalAddress in the primary FasterKV
            var primaryOutput = new PSFOutputPrimaryReadAddress<Key, Value>(this.fht.hlog, providerDatas);
            var psfArgs = new PSFReadArgs<Key, Value>(new PSFInputPrimaryReadAddress<Key>(logicalAddress), primaryOutput);
            return this.PsfReadAddress(ref psfArgs, this.ctx.serialNum + 1);
        }

        internal IEnumerable<FasterKVProviderData<Key, Value>> ReturnProviderDatas(IEnumerable<long> logicalAddresses)
        {
            // If the record is on disk the Read will go pending and we will not receive it "synchronously"
            // here; instead, it will work its way through the pending read system and call psfOutput.Visit.
            // providerDatas gives that a place to put the record. We should encounter this only after all
            // non-pending records have been read, but this approach allows any combination of pending and
            // non-pending reads.
            var providerDatas = new ConcurrentQueue<FasterKVProviderData<Key, Value>>();
            foreach (var logicalAddress in logicalAddresses)
            {
                var status = this.CreateProviderData(logicalAddress, providerDatas);
                if (status == Status.ERROR)
                {
                    // TODOerr: Handle error status from PsfReadAddress 
                }
                while (providerDatas.TryDequeue(out var providerData))
                    yield return providerData;
            }

            this.CompletePending(spinWait: true);
            while (providerDatas.TryDequeue(out var providerData))
                yield return providerData;
        }

        /// <summary>
        /// Issue a query on a single <see cref="PSF{TPSFKey, TRecordId}"/> on a single key value.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fht.QueryPSF(sizePsf, Size.Medium)) {...}
        /// </example>
        /// <typeparam name="TPSFKey">The type of the key value to return results for</typeparam>
        /// <param name="psf">The Predicate Subset Function object</param>
        /// <param name="key">The key value to return results for</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<Key, Value>> QueryPSF<TPSFKey>(
                PSF<TPSFKey, long> psf, TPSFKey key, PSFQuerySettings querySettings = null)
            where TPSFKey : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatas(this.fht.PSFManager.QueryPSF(psf, key, querySettings));
        }

        /// <summary>
        /// Issue a query on a single <see cref="PSF{TPSFKey, TRecordId}"/> on multiple key values.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fht.QueryPSF(sizePsf, new TestPSFKey[] { Size.Medium, Size.Large })) {...}
        /// (Note that this example requires an implicit TestPSFKey constructor taking Size).
        /// </example>
        /// <typeparam name="TPSFKey">The type of the key value to return results for</typeparam>
        /// <param name="psf">The Predicate Subset Function object</param>
        /// <param name="keys">A vector of key values to return results for; for example, an OR query on
        ///     a single PSF, or a range query for a PSF that generates keys identifying bins.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<Key, Value>> QueryPSF<TPSFKey>(
                PSF<TPSFKey, long> psf, IEnumerable<TPSFKey> keys, PSFQuerySettings querySettings = null)
            where TPSFKey : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatas(this.fht.PSFManager.QueryPSF(psf, keys, querySettings));
        }

        /// <summary>
        /// Issue a query on two <see cref="PSF{TPSFKey, TRecordId}"/>s, each with a single key value.
        /// </summary>
        /// <example>
        /// var providerData in fht.QueryPSF(sizePsf, Size.Medium, colorPsf, Color.Red, (l, r) => l || r))
        /// </example>
        /// <typeparam name="TPSFKey1">The type of the key value for the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key value for the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first Predicate Subset Function object</param>
        /// <param name="psf2">The second Predicate Subset Function object</param>
        /// <param name="key1">The key value to return results from the first <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="key2">The key value to return results from the second <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="matchPredicate">A predicate that takes as parameters 1) whether a candidate record matches
        /// the first PSF, 2) whether the record matches the second PSF, and returns a bool indicating whether the
        /// record should be part of the result set. For example, an AND query would return true iff both input
        /// parameters are true, else false; an OR query would return true if either input parameter is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<Key, Value>> QueryPSF<TPSFKey1, TPSFKey2>(
                    PSF<TPSFKey1, long> psf1, TPSFKey1 key1,
                    PSF<TPSFKey2, long> psf2, TPSFKey2 key2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatas(this.fht.PSFManager.QueryPSF(psf1, key1, psf2, key2, matchPredicate, querySettings));
        }

        /// <summary>
        /// Issue a query on two <see cref="PSF{TPSFKey, TRecordId}"/>s, each with a vector of key values.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fht.QueryPSF(
        ///         sizePsf, new [] { new SizeKey(Size.Medium), new SizeKey(Size.Large) },
        ///         colorPsf, new [] { new ColorKey(Color.Red), new ColorKey(Color.Blue) },
        ///         (l, r) => l || r))
        /// </example>
        /// <typeparam name="TPSFKey1">The type of the key value for the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key value for the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first Predicate Subset Function object</param>
        /// <param name="psf2">The secojnd Predicate Subset Function object</param>
        /// <param name="keys1">The key values to return results from the first <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="keys2">The key values to return results from the second <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="matchPredicate">A predicate that takes as parameters 1) whether a candidate record matches
        /// the first PSF, 2) whether the record matches the second PSF, and returns a bool indicating whether the
        /// record should be part of the result set. For example, an AND query would return true iff both input
        /// parameters are true, else false; an OR query would return true if either input parameter is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<Key, Value>> QueryPSF<TPSFKey1, TPSFKey2>(
                    PSF<TPSFKey1, long> psf1, IEnumerable<TPSFKey1> keys1,
                    PSF<TPSFKey2, long> psf2, IEnumerable<TPSFKey2> keys2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatas(this.fht.PSFManager.QueryPSF(psf1, keys1, psf2, keys2, matchPredicate, querySettings));
        }

        /// <summary>
        /// Issue a query on three <see cref="PSF{TPSFKey, TRecordId}"/>s, each with a single key value.
        /// </summary>
        /// <example>
        /// var providerData in fht.QueryPSF(sizePsf, Size.Medium, colorPsf, Color.Red, countPsf, 7, (l, m, r) => l || m || r))
        /// </example>
        /// <typeparam name="TPSFKey1">The type of the key value for the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key value for the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey3">The type of the key value for the third <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first Predicate Subset Function object</param>
        /// <param name="psf2">The second Predicate Subset Function object</param>
        /// <param name="psf3">The third Predicate Subset Function object</param>
        /// <param name="key1">The key value to return results from the first <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="key2">The key value to return results from the second <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="key3">The key value to return results from the third <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="matchPredicate">A predicate that takes as parameters 1) whether a candidate record matches
        /// the first PSF, 2) whether the record matches the second PSF, 3) whether the record matches the third PSF, and returns a bool indicating whether the
        /// record should be part of the result set. For example, an AND query would return true iff both input
        /// parameters are true, else false; an OR query would return true if either input parameter is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<Key, Value>> QueryPSF<TPSFKey1, TPSFKey2, TPSFKey3>(
                    PSF<TPSFKey1, long> psf1, TPSFKey1 key1,
                    PSF<TPSFKey2, long> psf2, TPSFKey2 key2,
                    PSF<TPSFKey3, long> psf3, TPSFKey3 key3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatas(this.fht.PSFManager.QueryPSF(psf1, key1, psf2, key2, psf3, key3, matchPredicate, querySettings));
        }

        /// <summary>
        /// Issue a query on three <see cref="PSF{TPSFKey, TRecordId}"/>s, each with a vector of key values.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fht.QueryPSF(
        ///         sizePsf, new [] { new SizeKey(Size.Medium), new SizeKey(Size.Large) },
        ///         colorPsf, new [] { new ColorKey(Color.Red), new ColorKey(Color.Blue) },
        ///         countPsf, new [] { new CountKey(7), new CountKey(42) },
        ///         (l, m, r) => l || m || r))
        /// </example>
        /// <typeparam name="TPSFKey1">The type of the key value for the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key value for the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey3">The type of the key value for the third <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first Predicate Subset Function object</param>
        /// <param name="psf2">The second Predicate Subset Function object</param>
        /// <param name="psf3">The third Predicate Subset Function object</param>
        /// <param name="keys1">The key values to return results from the first <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="keys2">The key values to return results from the second <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="keys3">The key values to return results from the third <see cref="PSF{TPSFKey, TRecordId}"/>'s stored values</param>
        /// <param name="matchPredicate">A predicate that takes as parameters 1) whether a candidate record matches
        /// the first PSF, 2) whether the record matches the second PSF, 3) whether the record matches the third PSF, and returns a bool indicating whether the
        /// record should be part of the result set. For example, an AND query would return true iff both input
        /// parameters are true, else false; an OR query would return true if either input parameter is true.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<Key, Value>> QueryPSF<TPSFKey1, TPSFKey2, TPSFKey3>(
                    PSF<TPSFKey1, long> psf1, IEnumerable<TPSFKey1> keys1,
                    PSF<TPSFKey2, long> psf2, IEnumerable<TPSFKey2> keys2,
                    PSF<TPSFKey3, long> psf3, IEnumerable<TPSFKey3> keys3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatas(this.fht.PSFManager.QueryPSF(psf1, keys1, psf2, keys2, psf3, keys3, matchPredicate, querySettings));
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
        /// (Note that this example requires an implicit TestPSFKey constructor taking Size).
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
                    IEnumerable<(PSF<TPSFKey, long> psf, IEnumerable<TPSFKey> keys)> psfsAndKeys,
                    Func<bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatas(this.fht.PSFManager.QueryPSF(psfsAndKeys, matchPredicate, querySettings));
        }

        /// <summary>
        /// Issue a query on multiple keys <see cref="PSF{TPSFKey, TRecordId}"/>s for two different key types.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fht.QueryPSF(
        ///         new[] {
        ///             (sizePsf, new TestPSFKey[] { Size.Medium, Size.Large }),
        ///             (colorPsf, new TestPSFKey[] { Color.Red, Color.Blue })},
        ///         new[] {
        ///             (countPsf, new [] { new CountKey(7), new CountKey(9) })},
        ///         (ll, rr) => ll[0] || rr[0]))
        /// (Note that this example requires an implicit TestPSFKey constructor taking Size).
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
                    IEnumerable<(PSF<TPSFKey1, long> psf, IEnumerable<TPSFKey1> keys)> psfsAndKeys1,
                    IEnumerable<(PSF<TPSFKey2, long> psf, IEnumerable<TPSFKey2> keys)> psfsAndKeys2,
                    Func<bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatas(this.fht.PSFManager.QueryPSF(psfsAndKeys1, psfsAndKeys2, matchPredicate, querySettings));
        }

        /// <summary>
        /// Issue a query on multiple keys <see cref="PSF{TPSFKey, TRecordId}"/>s for three different key types.
        /// </summary>
        /// <example>
        /// foreach (var providerData in fht.QueryPSF(
        ///         new[] { (sizePsf, new [] { new SizeKey(Size.Medium), new SizeKey(Size.Large) }) },
        ///         new[] { (colorPsf, new [] { new ColorKey(Color.Red), new ColorKey(Color.Blue) }) },
        ///         new[] { (countPsf, new [] { new CountKey(4), new CountKey(7) }) },
        ///         (ll, mm, rr) => ll[0] || mm[0] || rr[0]))
        /// </example>
        /// <typeparam name="TPSFKey1">The type of the key value for the first vector's <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPSFKey2">The type of the key value for the second vector's <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPSFKey3">The type of the key value for the third vector's <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <param name="psfsAndKeys1">A vector of <see cref="PSF{TPSFKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPSFKey1"/> to be queried</param>
        /// <param name="psfsAndKeys2">A vector of <see cref="PSF{TPSFKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPSFKey2"/> to be queried</param>
        /// <param name="psfsAndKeys3">A vector of <see cref="PSF{TPSFKey, TRecordId}"/>s and associated keys 
        /// of type <typeparamref name="TPSFKey3"/> to be queried</param>
        /// <param name="matchPredicate">A predicate that takes as a parameters three boolean vectors in parallel with 
        /// each other, and returns a bool indicating whether the record should be part of
        /// the result set. For example, an AND query would return true iff all elements of all input vectors are true,
        /// else false; an OR query would return true if any element of either input vector is true; and more complex
        /// logic could be done depending on the specific PSFs.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns>An enumerable of the FasterKV-specific provider data from the primary FasterKV 
        /// instance, as identified by the TRecordIds stored in the secondary FasterKV instances</returns>
        public IEnumerable<FasterKVProviderData<Key, Value>> QueryPSF<TPSFKey1, TPSFKey2, TPSFKey3>(
                    IEnumerable<(PSF<TPSFKey1, long> psf, IEnumerable<TPSFKey1> keys)> psfsAndKeys1,
                    IEnumerable<(PSF<TPSFKey2, long> psf, IEnumerable<TPSFKey2> keys)> psfsAndKeys2,
                    IEnumerable<(PSF<TPSFKey3, long> psf, IEnumerable<TPSFKey3> keys)> psfsAndKeys3,
                    Func<bool[], bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            // Unsafe(Resume|Suspend)Thread are done in the session.PsfRead* operations called by PSFGroup.QueryPSF.
            return this.ReturnProviderDatas(this.fht.PSFManager.QueryPSF(psfsAndKeys1, psfsAndKeys2, psfsAndKeys3, matchPredicate, querySettings));
        }
        #endregion PSF Query API for primary FasterKV
    }
}
