// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

//#define PSF_TRACE

using FASTER.core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FasterPSFSample
{
    public partial class FasterPSFSample
    {
        private const int UpsertCount = 100;
        private static int blueCount, mediumCount, bin7Count;
        private static int intersectMediumBlueCount, unionMediumBlueCount;
        private static int intersectMediumBlue7Count, unionMediumBlue7Count;
        private static int unionMediumLargeCount, unionRedBlueCount;
        private static int unionMediumLargeRedBlueCount, intersectMediumLargeRedBlueCount;
        
        internal static Dictionary<Key, IOrders> keyDict = new Dictionary<Key, IOrders>();

        internal static HashSet<Key> lastBinKeys = new HashSet<Key>();
        private static int initialSkippedLastBinCount;

        private static int nextId = 1000000000;

        internal static int serialNo;

        static async Task Main(string[] argv)
        {
            if (!ParseArgs(argv))
                return;

            if (useObjectValue)  // TODO add VarLenValue
                await RunSample<ObjectOrders, Input<ObjectOrders>, Output<ObjectOrders>, ObjectOrders.Functions, ObjectOrders.Serializer>();
            else
                await RunSample<BlittableOrders, Input<BlittableOrders>, Output<BlittableOrders>, BlittableOrders.Functions, NoSerializer>();
            return;
        }

        internal async static Task RunSample<TValue, TInput, TOutput, TFunctions, TSerializer>()
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            var fpsf = new FPSF<TValue, TInput, TOutput, TFunctions, TSerializer>(useObjectValue, useMultiGroups, useReadCache: false); // ReadCache and CopyReadsToTail are not supported for PSFs
            try
            {
                CountBinKey.WantLastBin = false;
                await RunInitialInserts(fpsf);
                CountBinKey.WantLastBin = true;
                await RunReads(fpsf);
                var ok = await QueryPSFsWithoutBoolOps(fpsf)
                        && await QueryPSFsWithBoolOps(fpsf)
                        && await UpdateSizeByUpsert(fpsf)
                        && await UpdateColorByRMW(fpsf)
                        && await UpdateCountByUpsert(fpsf)
                        && await Delete(fpsf);
                Console.WriteLine("--------------------------------------------------------");
                Console.WriteLine($"Completed run: UseObjects {useObjectValue}, MultiGroup {useMultiGroups}, Async {useAsync}");
                Console.WriteLine();
                Console.Write("===>>> ");
                Console.WriteLine(ok ? "Passed! All operations succeeded" 
                                     : "*** Failed! *** One or more operations did not succeed");
                Console.WriteLine();
                if (Debugger.IsAttached)
                {
                    Console.Write("Press ENTER to close this window . . .");
                    Console.ReadKey();
                }
            }
            finally
            {
                fpsf.Close();
            }

            //Console.WriteLine("Press <ENTER> to end");
            //Console.ReadLine();
        }

        [Conditional("PSF_TRACE")] static void PsfTrace(string message) => Console.Write(message);

        [Conditional("PSF_TRACE")] static void PsfTraceLine(string message) => Console.WriteLine(message);

        internal async static Task RunInitialInserts<TValue, TInput, TOutput, TFunctions, TSerializer>(FPSF<TValue, TInput, TOutput, TFunctions, TSerializer> fpsf)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine("Writing keys from 0 to {0} to FASTER", UpsertCount);

            var rng = new Random(13);
            using var session = fpsf.FasterKV.NewSession<TInput, TOutput, Context<TValue>, TFunctions>(new TFunctions());
            var context = new Context<TValue>();
            var input = default(TInput);

            for (int ii = 0; ii < UpsertCount; ii++)
            {
                // Leave the last value unassigned from each category (we'll use it to update later)
                var key = new Key(Interlocked.Increment(ref nextId) - 1);
                var value = new TValue
                {
                    Id = key.Id,
                    SizeInt = rng.Next((int)Constants.Size.NumSizes - 1),
                    ColorArgb = Constants.Colors[rng.Next(Constants.Colors.Length - 1)].ToArgb(),
                    Count = rng.Next(CountBinKey.MaxOrders - 1)
                };

                keyDict[key] = value;
                CountBinKey.GetBin(value.Count, out int bin);
                var isBlue = value.ColorArgb == Color.Blue.ToArgb();
                var isRed = value.ColorArgb == Color.Red.ToArgb();
                var isMedium = value.SizeInt == (int)Constants.Size.Medium;
                var isLarge = value.SizeInt == (int)Constants.Size.Large;
                var isBin7 = bin == 7;

                if (isBlue)
                {
                    ++blueCount;
                    if (isMedium)
                    {
                        ++intersectMediumBlueCount;
                        if (isBin7)
                            ++intersectMediumBlue7Count;
                    }
                }
                if (isMedium)
                    ++mediumCount;

                if (isBin7) 
                    ++bin7Count;
                else if (bin == CountBinKey.LastBin)
                    lastBinKeys.Add(key);

                if (isMedium || isBlue)
                    ++unionMediumBlueCount;
                if (isMedium || isBlue || isBin7)
                    ++unionMediumBlue7Count;

                var isMediumOrLarge = isMedium || isLarge;
                var isRedOrBlue = isBlue || isRed;

                if (isMediumOrLarge)
                {
                    ++unionMediumLargeCount;
                    if (isRedOrBlue)
                        ++intersectMediumLargeRedBlueCount;
                }
                if (isRedOrBlue)
                    ++unionRedBlueCount;
                if (isMediumOrLarge || isRedOrBlue)
                    ++unionMediumLargeRedBlueCount;

                PsfTrace($"{value} |");

                // Both Upsert and RMW do an insert when the key is not found.
                if ((ii & 1) == 0)
                {
                    if (useAsync)
                        await session.UpsertAsync(ref key, ref value, context);
                    else
                        session.Upsert(ref key, ref value, context, serialNo);
                }
                else
                {
                    input.InitialUpdateValue = value;
                    if (useAsync)
                        await session.RMWAsync(ref key, ref input, context);
                    else
                        session.RMW(ref key, ref input, context, serialNo);
                }
            }
            ++serialNo;

            initialSkippedLastBinCount = lastBinKeys.Count();
            Console.WriteLine($"Upserted {UpsertCount} elements");
        }

        private static void RemoveIfSkippedLastBinKey(ref Key key)
        {
            // TODO: If we can do IPU in PSFs rather than RCU, we'll need to see what was actually updated.
            if (!useMultiGroups)
                lastBinKeys.Remove(key);
        }

        internal async static Task RunReads<TValue, TInput, TOutput, TFunctions, TSerializer>(FPSF<TValue, TInput, TOutput, TFunctions, TSerializer> fpsf)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine("Reading {0} random keys from FASTER", UpsertCount);

            var rng = new Random(0);
            int statusPending = 0;
            var output = new TOutput();
            var input = default(TInput);
            var context = new Context<TValue>();
            var readCount = UpsertCount * 2;

            var keys = keyDict.Keys.ToArray();

            using var session = fpsf.FasterKV.NewSession<TInput, TOutput, Context<TValue>, TFunctions>(new TFunctions());
            for (int i = 0; i < UpsertCount; i++)
            {
                var key = keys[rng.Next(keys.Length)];
                var status = Status.OK;
                if (useAsync)
                {
                    (status, output) = (await session.ReadAsync(ref key, ref input, context)).CompleteRead();
                }
                else
                {
                    session.Read(ref key, ref input, ref output, context, serialNo);
                }

                if (status == Status.OK && output.Value.MemberTuple != key.MemberTuple)
                    throw new Exception($"Error: Value does not match key in {nameof(RunReads)}");
            }
            ++serialNo;

            session.CompletePending(true);
            Console.WriteLine($"Read {readCount} elements with {++statusPending} Pending");
        }

        const string indent2 = "  ";
        const string indent4 = "    ";

        static bool VerifyProviderDatas<TValue>(FasterKVProviderData<Key, TValue>[] providerDatas, string name, int expectedCount)
            where TValue : IOrders, new()
        {
            Console.Write($"{indent4}{name}: ");
            if (verbose)
            {
                foreach (var providerData in providerDatas)
                {
                    ref TValue value = ref providerData.GetValue();
                    Console.WriteLine(indent4 + value);
                }
            }
            Console.WriteLine(providerDatas.Length == expectedCount
                              ? $"Passed: expected == actual ({expectedCount})"
                              : $"Failed: expected ({expectedCount}) != actual ({providerDatas.Length})");
            return expectedCount == providerDatas.Length;
        }

        internal async static Task<bool> QueryPSFsWithoutBoolOps<TValue, TInput, TOutput, TFunctions, TSerializer>(FPSF<TValue, TInput, TOutput, TFunctions, TSerializer> fpsf)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            Console.WriteLine("Querying PSFs from FASTER with no boolean ops", UpsertCount);

            using var session = fpsf.FasterKV.NewSession<TInput, TOutput, Context<TValue>, TFunctions>(new TFunctions());
            FasterKVProviderData<Key, TValue>[] providerDatas = null;
            var ok = true;

            async Task<FasterKVProviderData<Key, TValue>[]> RunQuery<TPSFKey>(IPSF psf, TPSFKey key) where TPSFKey : struct
                => useAsync
                    ? await session.QueryPSFAsync(psf, key).ToArrayAsync()
                    : session.QueryPSF(psf, key).ToArray();

            providerDatas = useMultiGroups
                                ? await RunQuery(fpsf.SizePsf, new SizeKey(Constants.Size.Medium))
                                : await RunQuery(fpsf.CombinedSizePsf, new CombinedKey(Constants.Size.Medium));
            ok &= VerifyProviderDatas(providerDatas, "Medium", mediumCount);
            
            providerDatas = useMultiGroups
                                ? await RunQuery(fpsf.ColorPsf, new ColorKey(Color.Blue))
                                : await RunQuery(fpsf.CombinedColorPsf, new CombinedKey(Color.Blue));
            ok &= VerifyProviderDatas(providerDatas, "Blue", blueCount);

            providerDatas = useMultiGroups
                                ? await RunQuery(fpsf.CountBinPsf, new CountBinKey(7))
                                : await RunQuery(fpsf.CombinedCountBinPsf, new CombinedKey(7));
            ok &= VerifyProviderDatas(providerDatas, "Bin7", bin7Count);

            providerDatas = useMultiGroups
                                ? await RunQuery(fpsf.CountBinPsf, new CountBinKey(CountBinKey.LastBin))
                                : await RunQuery(fpsf.CombinedCountBinPsf, new CombinedKey(CountBinKey.LastBin));
            ok &= VerifyProviderDatas(providerDatas, "LastBin", 0); // Insert skipped (returned null from the PSF) all that fall into the last bin
            return ok;
        }

        internal async static Task<bool> QueryPSFsWithBoolOps<TValue, TInput, TOutput, TFunctions, TSerializer>(FPSF<TValue, TInput, TOutput, TFunctions, TSerializer> fpsf)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            Console.WriteLine("Querying PSFs from FASTER with boolean ops", UpsertCount);

            using var session = fpsf.FasterKV.NewSession<TInput, TOutput, Context<TValue>, TFunctions>(new TFunctions());
            FasterKVProviderData<Key, TValue>[] providerDatas = null;
            var ok = true;

            // Local functions can't be overloaded so make the name unique
            async Task<FasterKVProviderData<Key, TValue>[]> RunQuery2<TPSFKey1, TPSFKey2>(
                    IPSF psf1, TPSFKey1 key1,
                    IPSF psf2, TPSFKey2 key2,
                    Func<bool, bool, bool> matchPredicate)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
                => useAsync
                    ? await session.QueryPSFAsync(psf1, key1, psf2, key2, matchPredicate).ToArrayAsync()
                    : session.QueryPSF(psf1, key1, psf2, key2, matchPredicate).ToArray();

            async Task<FasterKVProviderData<Key, TValue>[]> RunQuery3<TPSFKey1, TPSFKey2, TPSFKey3>(
                    IPSF psf1, TPSFKey1 key1,
                    IPSF psf2, TPSFKey2 key2,
                    IPSF psf3, TPSFKey3 key3,
                    Func<bool, bool, bool, bool> matchPredicate)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
                => useAsync
                    ? await session.QueryPSFAsync(psf1, key1, psf2, key2, psf3, key3, matchPredicate).ToArrayAsync()
                    : session.QueryPSF(psf1, key1, psf2, key2, psf3, key3, matchPredicate).ToArray();

            async Task<FasterKVProviderData<Key, TValue>[]> RunQuery1EnumKeys<TPSFKey>(
                IPSF psf, IEnumerable<TPSFKey> keys, PSFQuerySettings querySettings = null)
            where TPSFKey : struct
                => useAsync
                    ? await session.QueryPSFAsync(psf, keys).ToArrayAsync()
                    : session.QueryPSF(psf, keys).ToArray();

            async Task<FasterKVProviderData<Key, TValue>[]> RunQuery2Vec<TPSFKey1, TPSFKey2>(
                    IPSF psf1, IEnumerable<TPSFKey1> keys1,
                    IPSF psf2, IEnumerable<TPSFKey2> keys2,
                    Func<bool, bool, bool> matchPredicate)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
                => useAsync
                    ? await session.QueryPSFAsync(psf1, keys1, psf2, keys2, matchPredicate).ToArrayAsync()
                    : session.QueryPSF(psf1, keys1, psf2, keys2, matchPredicate).ToArray();

            async Task<FasterKVProviderData<Key, TValue>[]> RunQuery1EnumTuple<TPSFKey>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey> keys)> psfsAndKeys,
                    Func<bool[], bool> matchPredicate)
            where TPSFKey : struct
                => useAsync
                    ? await session.QueryPSFAsync(psfsAndKeys, matchPredicate).ToArrayAsync()
                    : session.QueryPSF(psfsAndKeys, matchPredicate).ToArray();

            if (useMultiGroups)
            {
                providerDatas = await RunQuery2(fpsf.SizePsf, new SizeKey(Constants.Size.Medium),
                                               fpsf.ColorPsf, new ColorKey(Color.Blue), (sz, cl) => sz && cl);
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumBlueCount), intersectMediumBlueCount);
                providerDatas = await RunQuery2(fpsf.SizePsf, new SizeKey(Constants.Size.Medium),
                                               fpsf.ColorPsf, new ColorKey(Color.Blue), (sz, cl) => sz || cl);
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumBlueCount), unionMediumBlueCount);

                providerDatas = await RunQuery3(fpsf.SizePsf, new SizeKey(Constants.Size.Medium),
                                                fpsf.ColorPsf, new ColorKey(Color.Blue),
                                                fpsf.CountBinPsf, new CountBinKey(7), (sz, cl, ct) => sz && cl && ct);
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumBlue7Count), intersectMediumBlue7Count);
                providerDatas = await RunQuery3(fpsf.SizePsf, new SizeKey(Constants.Size.Medium),
                                                fpsf.ColorPsf, new ColorKey(Color.Blue),
                                                fpsf.CountBinPsf, new CountBinKey(7), (sz, cl, ct) => sz || cl || ct);
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumBlue7Count), unionMediumBlue7Count);

                providerDatas = await RunQuery1EnumKeys(fpsf.SizePsf, new[] { new SizeKey(Constants.Size.Medium), new SizeKey(Constants.Size.Large) });
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumLargeCount), unionMediumLargeCount);
                providerDatas = await RunQuery1EnumKeys(fpsf.ColorPsf, new[] { new ColorKey(Color.Blue), new ColorKey(Color.Red) });
                ok &= VerifyProviderDatas(providerDatas, nameof(unionRedBlueCount), unionRedBlueCount);

                providerDatas = await RunQuery2Vec(fpsf.SizePsf, new[] { new SizeKey(Constants.Size.Medium), new SizeKey(Constants.Size.Large) },
                                                 fpsf.ColorPsf, new[] { new ColorKey(Color.Blue), new ColorKey(Color.Red) }, (sz, cl) => sz && cl);
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumLargeRedBlueCount), intersectMediumLargeRedBlueCount);
                providerDatas = await RunQuery2Vec(fpsf.SizePsf, new[] { new SizeKey(Constants.Size.Medium), new SizeKey(Constants.Size.Large) },
                                                 fpsf.ColorPsf, new[] { new ColorKey(Color.Blue), new ColorKey(Color.Red) }, (sz, cl) => sz || cl);
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumLargeRedBlueCount), unionMediumLargeRedBlueCount);
            }
            else
            {
                providerDatas = await RunQuery2(fpsf.CombinedSizePsf, new CombinedKey(Constants.Size.Medium),
                                               fpsf.CombinedColorPsf, new CombinedKey(Color.Blue), (sz, cl) => sz && cl);
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumBlueCount), intersectMediumBlueCount);
                providerDatas = await RunQuery1EnumTuple(new[] { (fpsf.CombinedSizePsf, new[] { new CombinedKey(Constants.Size.Medium) }.AsEnumerable()),
                                                         (fpsf.CombinedColorPsf, new[] { new CombinedKey(Color.Blue) }.AsEnumerable()) }, sz => sz[0] && sz[1]);
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumBlueCount), intersectMediumBlueCount);
                // ---
                providerDatas = await RunQuery2(fpsf.CombinedSizePsf, new CombinedKey(Constants.Size.Medium),
                                               fpsf.CombinedColorPsf, new CombinedKey(Color.Blue), (sz, cl) => sz || cl);
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumBlueCount), unionMediumBlueCount);
                providerDatas = await RunQuery1EnumTuple(new[] { (fpsf.CombinedSizePsf, new[] { new CombinedKey(Constants.Size.Medium) }.AsEnumerable()),
                                                         (fpsf.CombinedColorPsf, new[] { new CombinedKey(Color.Blue) }.AsEnumerable()) }, sz => sz[0] || sz[1]);
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumBlueCount), unionMediumBlueCount);
                // ---
                providerDatas = await RunQuery3(fpsf.CombinedSizePsf, new CombinedKey(Constants.Size.Medium),
                                                fpsf.CombinedColorPsf, new CombinedKey(Color.Blue),
                                                fpsf.CombinedCountBinPsf, new CombinedKey(7), (sz, cl, ct) => sz && cl && ct);
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumBlue7Count), intersectMediumBlue7Count);
                providerDatas = await RunQuery1EnumTuple(new[] {(fpsf.CombinedSizePsf, new [] { new CombinedKey(Constants.Size.Medium) }.AsEnumerable()),
                                                         (fpsf.CombinedColorPsf, new [] { new CombinedKey(Color.Blue) }.AsEnumerable()),
                                                         (fpsf.CombinedCountBinPsf, new [] { new CombinedKey(7) }.AsEnumerable()) }, sz => sz[0] && sz[1] && sz[2]);
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumBlue7Count), intersectMediumBlue7Count);
                // ---
                providerDatas = await RunQuery3(fpsf.CombinedSizePsf, new CombinedKey(Constants.Size.Medium),
                                                fpsf.CombinedColorPsf, new CombinedKey(Color.Blue),
                                                fpsf.CombinedCountBinPsf, new CombinedKey(7), (sz, cl, ct) => sz || cl || ct);
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumBlue7Count), unionMediumBlue7Count);
                providerDatas = await RunQuery1EnumTuple(new[] {(fpsf.CombinedSizePsf, new [] { new CombinedKey(Constants.Size.Medium) }.AsEnumerable()),
                                                         (fpsf.CombinedColorPsf, new [] { new CombinedKey(Color.Blue) }.AsEnumerable()),
                                                         (fpsf.CombinedCountBinPsf, new [] { new CombinedKey(7) }.AsEnumerable()) }, sz => sz[0] || sz[1] || sz[2]);
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumBlue7Count), unionMediumBlue7Count);
                // ---
                providerDatas = await RunQuery1EnumKeys(fpsf.CombinedSizePsf, new[] { new CombinedKey(Constants.Size.Medium), new CombinedKey(Constants.Size.Large) });
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumLargeCount), unionMediumLargeCount);
                providerDatas = await RunQuery1EnumKeys(fpsf.CombinedColorPsf, new[] { new CombinedKey(Color.Blue), new CombinedKey(Color.Red) });
                ok &= VerifyProviderDatas(providerDatas, nameof(unionRedBlueCount), unionRedBlueCount);
                // ---
                providerDatas = await RunQuery2Vec(fpsf.CombinedSizePsf, new[] { new CombinedKey(Constants.Size.Medium), new CombinedKey(Constants.Size.Large) },
                                                 fpsf.CombinedColorPsf, new[] { new CombinedKey(Color.Blue), new CombinedKey(Color.Red) }, (sz, cl) => sz && cl);
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumLargeRedBlueCount), intersectMediumLargeRedBlueCount);
                providerDatas = await RunQuery1EnumTuple(new[] {(fpsf.CombinedSizePsf, new[] { new CombinedKey(Constants.Size.Medium), new CombinedKey(Constants.Size.Large) }.AsEnumerable()),
                                                         (fpsf.CombinedColorPsf, new[] { new CombinedKey(Color.Blue), new CombinedKey(Color.Red) }.AsEnumerable())}, sz => sz[0] && sz[1]);
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumLargeRedBlueCount), intersectMediumLargeRedBlueCount);
                // ---
                providerDatas = await RunQuery2Vec(fpsf.CombinedSizePsf, new[] { new CombinedKey(Constants.Size.Medium), new CombinedKey(Constants.Size.Large) },
                                                 fpsf.CombinedColorPsf, new[] { new CombinedKey(Color.Blue), new CombinedKey(Color.Red) }, (sz, cl) => sz || cl);
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumLargeRedBlueCount), unionMediumLargeRedBlueCount);
                providerDatas = await RunQuery1EnumTuple(new[] {(fpsf.CombinedSizePsf, new[] { new CombinedKey(Constants.Size.Medium), new CombinedKey(Constants.Size.Large) }.AsEnumerable()),
                                                         (fpsf.CombinedColorPsf, new[] { new CombinedKey(Color.Blue), new CombinedKey(Color.Red) }.AsEnumerable())}, sz => sz[0] || sz[1]);
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumLargeRedBlueCount), unionMediumLargeRedBlueCount);
            }

            return ok;
        }

        private static void WriteResult(bool isInitial, string name, int expectedCount, int actualCount)
        {
            var tag = isInitial ? "Initial" : "Updated";
            Console.WriteLine(expectedCount == actualCount
                                ? $"{indent4}{tag} {name} Passed: expected == actual ({expectedCount})"
                                : $"{indent4}{tag} {name} Failed: expected ({expectedCount}) != actual ({actualCount})");
        }

        internal async static Task<bool> UpdateSizeByUpsert<TValue, TInput, TOutput, TFunctions, TSerializer>(FPSF<TValue, TInput, TOutput, TFunctions, TSerializer> fpsf)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            Console.WriteLine("Updating Sizes via Upsert");

            using var session = fpsf.FasterKV.NewSession<TInput, TOutput, Context<TValue>, TFunctions>(new TFunctions());

            FasterKVProviderData<Key, TValue>[] GetSizeDatas(Constants.Size size)
                => useMultiGroups
                    ? session.QueryPSF(fpsf.SizePsf, new SizeKey(size)).ToArray()
                    : session.QueryPSF(fpsf.CombinedSizePsf, new CombinedKey(size)).ToArray();

            var xxlDatas = GetSizeDatas(Constants.Size.XXLarge);
            WriteResult(isInitial: true, "XXLarge", 0, xxlDatas.Length);
            var mediumDatas = GetSizeDatas(Constants.Size.Medium);
            WriteResult(isInitial: true, "Medium", mediumCount, mediumDatas.Length);
            var expected = mediumDatas.Length;
            Console.WriteLine($"{indent2}Changing all Medium to XXLarge");

            var context = new Context<TValue>();
            foreach (var providerData in mediumDatas)
            {
                // Get the old value and confirm it's as expected. We cannot have ref locals because this is an async function.
                Debug.Assert(providerData.GetValue().SizeInt == (int)Constants.Size.Medium);

                // Clone the old value with updated Size; note that this cannot modify the "ref providerData.GetValue()" in-place as that will bypass PSFs.
                var newValue = new TValue
                {
                    Id = providerData.GetValue().Id,
                    SizeInt = (int)Constants.Size.XXLarge, // updated
                    ColorArgb = providerData.GetValue().ColorArgb,
                    Count = providerData.GetValue().Count
                };

                // Reuse the same key
                if (useAsync)
                    await session.UpsertAsync(ref providerData.GetKey(), ref newValue, context);
                else
                    session.Upsert(ref providerData.GetKey(), ref newValue, context, serialNo);

                RemoveIfSkippedLastBinKey(ref providerData.GetKey());
            }
            ++serialNo;

            xxlDatas = GetSizeDatas(Constants.Size.XXLarge);
            mediumDatas = GetSizeDatas(Constants.Size.Medium);
            bool ok = xxlDatas.Length == expected && mediumDatas.Length == 0;
            WriteResult(isInitial: false, "XXLarge", expected, xxlDatas.Length);
            WriteResult(isInitial: false, "Medium", 0, mediumDatas.Length);
            return ok;
        }

        internal async static Task<bool> UpdateColorByRMW<TValue, TInput, TOutput, TFunctions, TSerializer>(FPSF<TValue, TInput, TOutput, TFunctions, TSerializer> fpsf)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            Console.WriteLine("Updating Colors via RMW");

            using var session = fpsf.FasterKV.NewSession<TInput, TOutput, Context<TValue>, TFunctions>(new TFunctions());

            FasterKVProviderData<Key, TValue>[] GetColorDatas(Color color)
                => useMultiGroups
                    ? session.QueryPSF(fpsf.ColorPsf, new ColorKey(color)).ToArray()
                    : session.QueryPSF(fpsf.CombinedColorPsf, new CombinedKey(color)).ToArray();

            var purpleDatas = GetColorDatas(Color.Purple);
            WriteResult(isInitial: true, "Purple", 0, purpleDatas.Length);
            var blueDatas = GetColorDatas(Color.Blue);
            WriteResult(isInitial: true, "Blue", blueCount, blueDatas.Length);
            var expected = blueDatas.Length;
            Console.WriteLine($"{indent2}Changing all Blue to Purple");

            var context = new Context<TValue>();
            var input = new TInput { IPUColorInt = Color.Purple.ToArgb() };
            foreach (var providerData in blueDatas)
            {
                // This will call Functions<>.InPlaceUpdater.
                if (useAsync)
                    await session.RMWAsync(ref providerData.GetKey(), ref input, context);
                else
                    session.RMW(ref providerData.GetKey(), ref input, context, serialNo);

                RemoveIfSkippedLastBinKey(ref providerData.GetKey());
            }
            ++serialNo;

            purpleDatas = GetColorDatas(Color.Purple);
            blueDatas = GetColorDatas(Color.Blue);
            bool ok = purpleDatas.Length == expected && blueDatas.Length == 0;
            WriteResult(isInitial: false, "Purple", expected, purpleDatas.Length);
            WriteResult(isInitial: false, "Blue", 0, blueDatas.Length);
            return ok;
        }

        internal async static Task<bool> UpdateCountByUpsert<TValue, TInput, TOutput, TFunctions, TSerializer>(FPSF<TValue, TInput, TOutput, TFunctions, TSerializer> fpsf)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            Console.WriteLine("Updating Counts via Upsert");

            using var session = fpsf.FasterKV.NewSession<TInput, TOutput, Context<TValue>, TFunctions>(new TFunctions());

            var bin7 = 7;
            FasterKVProviderData<Key, TValue>[] GetCountDatas(int bin)
                => useMultiGroups
                    ? session.QueryPSF(fpsf.CountBinPsf, new CountBinKey(bin)).ToArray()
                    : session.QueryPSF(fpsf.CombinedCountBinPsf, new CombinedKey(bin)).ToArray();

            // First show we've nothing in the last bin, and get all in bin7.
            var lastBinDatas = GetCountDatas(CountBinKey.LastBin);
            int expectedLastBinCount = initialSkippedLastBinCount - lastBinKeys.Count();
            var ok = lastBinDatas.Length == expectedLastBinCount;
            WriteResult(isInitial: true, "LastBin", expectedLastBinCount, lastBinDatas.Length);

            var bin7Datas = GetCountDatas(bin7);
            ok &= bin7Datas.Length == bin7Count;
            WriteResult(isInitial: true, "Bin7", bin7Count, bin7Datas.Length);

            Console.WriteLine($"{indent2}Changing all Bin7 to LastBin");
            var context = new Context<TValue>();
            foreach (var providerData in bin7Datas)
            {
                // Get the old value and confirm it's as expected. We cannot have ref locals because this is an async function.
                Debug.Assert(CountBinKey.GetBin(providerData.GetValue().Count, out int tempBin) && tempBin == bin7);

                // Clone the old value with updated Count; note that this cannot modify the "ref providerData.GetValue()" in-place as that will bypass PSFs.
                var newValue = new TValue
                {
                    Id = providerData.GetValue().Id,
                    SizeInt = providerData.GetValue().SizeInt,
                    ColorArgb = providerData.GetValue().ColorArgb,
                    Count = providerData.GetValue().Count + (CountBinKey.LastBin - bin7) * CountBinKey.BinSize // updated
                };
                Debug.Assert(CountBinKey.GetBin(newValue.Count, out tempBin) && tempBin == CountBinKey.LastBin);

                // Reuse the same key
                if (useAsync)
                    await session.UpsertAsync(ref providerData.GetKey(), ref newValue, context);
                else
                    session.Upsert(ref providerData.GetKey(), ref newValue, context, serialNo);
            }
            ++serialNo;

            expectedLastBinCount += bin7Datas.Length;
            lastBinDatas = GetCountDatas(CountBinKey.LastBin);
            ok &= lastBinDatas.Length == expectedLastBinCount;
            WriteResult(isInitial: false, "LastBin", expectedLastBinCount, lastBinDatas.Length);

            bin7Datas = GetCountDatas(bin7);
            ok &= bin7Datas.Length == 0;
            WriteResult(isInitial: false, "Bin7", 0, bin7Datas.Length);
            return ok;
        }

        internal async static Task<bool> Delete<TValue, TInput, TOutput, TFunctions, TSerializer>(FPSF<TValue, TInput, TOutput, TFunctions, TSerializer> fpsf)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            Console.WriteLine("Deleting Colors");

            using var session = fpsf.FasterKV.NewSession<TInput, TOutput, Context<TValue>, TFunctions>(new TFunctions());

            async Task<FasterKVProviderData<Key, TValue>[]> GetColorDatas(Color color)
                => useMultiGroups
                    ? useAsync 
                        ? await session.QueryPSFAsync(fpsf.ColorPsf, new ColorKey(color)).ToArrayAsync()
                        : session.QueryPSF(fpsf.ColorPsf, new ColorKey(color)).ToArray()
                    : useAsync
                        ? await session.QueryPSFAsync(fpsf.CombinedColorPsf, new CombinedKey(color)).ToArrayAsync()
                        : session.QueryPSF(fpsf.CombinedColorPsf, new CombinedKey(color)).ToArray();

            var redDatas = await GetColorDatas(Color.Red);
            Console.WriteLine();
            Console.Write($"Deleting all Reds; initial count {redDatas.Length}");

            var context = new Context<TValue>();
            foreach (var providerData in redDatas)
            {
                // This will call Functions<>.InPlaceUpdater.
                if (useAsync)
                    await session.DeleteAsync(ref providerData.GetKey(), context);
                else
                    session.Delete(ref providerData.GetKey(), context, serialNo);
            }
            ++serialNo;
            Console.WriteLine();

            redDatas = await GetColorDatas(Color.Red);
            var ok = redDatas.Length == 0;
            Console.Write(ok ? "Passed" : "*** Failed *** ");
            Console.WriteLine($": Red {redDatas.Length}");
            return ok;
        }
    }
}
