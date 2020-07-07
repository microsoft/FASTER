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

        static void Main(string[] argv)
        {
            if (!ParseArgs(argv))
                return;

            if (useObjectValue)  // TODO add VarLenValue
                RunSample<ObjectOrders, Input<ObjectOrders>, Output<ObjectOrders>, ObjectOrders.Functions, ObjectOrders.Serializer>();
            else
                RunSample<BlittableOrders, Input<BlittableOrders>, Output<BlittableOrders>, BlittableOrders.Functions, NoSerializer>();
            return;
        }

        internal static void RunSample<TValue, TInput, TOutput, TFunctions, TSerializer>()
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            var fpsf = new FPSF<TValue, TInput, TOutput, TFunctions, TSerializer>(useObjectValue, useMultiGroups, useReadCache: true);
            try
            {
                CountBinKey.WantLastBin = false;
                RunInitialInserts(fpsf);
                CountBinKey.WantLastBin = true;
                RunReads(fpsf);
                var ok = QueryPSFsWithoutBoolOps(fpsf)
                        && QueryPSFsWithBoolOps(fpsf)
                        && UpdateSizeByUpsert(fpsf)
                        && UpdateColorByRMW(fpsf)
                        && UpdateCountByUpsert(fpsf)
                        && Delete(fpsf);
                Console.WriteLine("--------------------------------------------------------");
                Console.WriteLine(ok ? "Passed! All operations succeeded" 
                                     : "*** Failed! *** One or more operations did not succeed");
                Console.WriteLine();
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

        internal static void RunInitialInserts<TValue, TInput, TOutput, TFunctions, TSerializer>(FPSF<TValue, TInput, TOutput, TFunctions, TSerializer> fpsf)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine("Writing keys from 0 to {0} to FASTER", UpsertCount);

            var rng = new Random(13);
            using var session = fpsf.fht.NewSession();
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
                    session.Upsert(ref key, ref value, context, serialNo);
                }
                else
                {
                    input.InitialUpdateValue = value;
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

        internal static void RunReads<TValue, TInput, TOutput, TFunctions, TSerializer>(FPSF<TValue, TInput, TOutput, TFunctions, TSerializer> fpsf)
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

            using var session = fpsf.fht.NewSession();
            for (int i = 0; i < UpsertCount; i++)
            {
                var key = keys[rng.Next(keys.Length)];
                var status = session.Read(ref key, ref input, ref output, context, serialNo);

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

        internal static bool QueryPSFsWithoutBoolOps<TValue, TInput, TOutput, TFunctions, TSerializer>(FPSF<TValue, TInput, TOutput, TFunctions, TSerializer> fpsf)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            Console.WriteLine("Querying PSFs from FASTER with no boolean ops", UpsertCount);

            using var session = fpsf.fht.NewSession();
            FasterKVProviderData<Key, TValue>[] providerDatas = null;
            var ok = true;

            providerDatas = useMultiGroups
                                ? session.QueryPSF(fpsf.SizePsf, new SizeKey(Constants.Size.Medium)).ToArray()
                                : session.QueryPSF(fpsf.CombinedSizePsf, new CombinedKey(Constants.Size.Medium)).ToArray();
            ok &= VerifyProviderDatas(providerDatas, "Medium", mediumCount);
            
            providerDatas = useMultiGroups
                                ? session.QueryPSF(fpsf.ColorPsf, new ColorKey(Color.Blue)).ToArray()
                                : session.QueryPSF(fpsf.CombinedColorPsf, new CombinedKey(Color.Blue)).ToArray();
            ok &= VerifyProviderDatas(providerDatas, "Blue", blueCount);

            providerDatas = useMultiGroups
                                ? session.QueryPSF(fpsf.CountBinPsf, new CountBinKey(7)).ToArray()
                                : session.QueryPSF(fpsf.CombinedCountBinPsf, new CombinedKey(7)).ToArray();
            ok &= VerifyProviderDatas(providerDatas, "Bin7", bin7Count);

            providerDatas = useMultiGroups
                                ? session.QueryPSF(fpsf.CountBinPsf, new CountBinKey(CountBinKey.LastBin)).ToArray()
                                : session.QueryPSF(fpsf.CombinedCountBinPsf, new CombinedKey(CountBinKey.LastBin)).ToArray();
            ok &= VerifyProviderDatas(providerDatas, "LastBin", 0); // Insert skipped (returned null from the PSF) all that fall into the last bin
            return ok;
        }

        internal static bool QueryPSFsWithBoolOps<TValue, TInput, TOutput, TFunctions, TSerializer>(FPSF<TValue, TInput, TOutput, TFunctions, TSerializer> fpsf)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            Console.WriteLine("Querying PSFs from FASTER with boolean ops", UpsertCount);

            using var session = fpsf.fht.NewSession();
            FasterKVProviderData<Key, TValue>[] providerDatas = null;
            var ok = true;

            if (useMultiGroups)
            {
                providerDatas = session.QueryPSF(fpsf.SizePsf, new SizeKey(Constants.Size.Medium),
                                                 fpsf.ColorPsf, new ColorKey(Color.Blue), (sz, cl) => sz && cl).ToArray();
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumBlueCount), intersectMediumBlueCount);
                providerDatas = session.QueryPSF(fpsf.SizePsf, new SizeKey(Constants.Size.Medium),
                                                 fpsf.ColorPsf, new ColorKey(Color.Blue), (sz, cl) => sz || cl).ToArray();
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumBlueCount), unionMediumBlueCount);

                providerDatas = session.QueryPSF(fpsf.SizePsf, new SizeKey(Constants.Size.Medium),
                                                 fpsf.ColorPsf, new ColorKey(Color.Blue),
                                                 fpsf.CountBinPsf, new CountBinKey(7), (sz, cl, ct) => sz && cl && ct).ToArray();
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumBlue7Count), intersectMediumBlue7Count);
                providerDatas = session.QueryPSF(fpsf.SizePsf, new SizeKey(Constants.Size.Medium),
                                                 fpsf.ColorPsf, new ColorKey(Color.Blue),
                                                 fpsf.CountBinPsf, new CountBinKey(7), (sz, cl, ct) => sz || cl || ct).ToArray();
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumBlue7Count), unionMediumBlue7Count);

                providerDatas = session.QueryPSF(fpsf.SizePsf, new[] { new SizeKey(Constants.Size.Medium), new SizeKey(Constants.Size.Large) }).ToArray();
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumLargeCount), unionMediumLargeCount);
                providerDatas = session.QueryPSF(fpsf.ColorPsf, new[] { new ColorKey(Color.Blue), new ColorKey(Color.Red) }).ToArray();
                ok &= VerifyProviderDatas(providerDatas, nameof(unionRedBlueCount), unionRedBlueCount);

                providerDatas = session.QueryPSF(fpsf.SizePsf, new[] { new SizeKey(Constants.Size.Medium), new SizeKey(Constants.Size.Large) },
                                                 fpsf.ColorPsf, new[] { new ColorKey(Color.Blue), new ColorKey(Color.Red) }, (sz, cl) => sz && cl).ToArray();
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumLargeRedBlueCount), intersectMediumLargeRedBlueCount);
                providerDatas = session.QueryPSF(fpsf.SizePsf, new[] { new SizeKey(Constants.Size.Medium), new SizeKey(Constants.Size.Large) },
                                                 fpsf.ColorPsf, new[] { new ColorKey(Color.Blue), new ColorKey(Color.Red) }, (sz, cl) => sz || cl).ToArray();
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumLargeRedBlueCount), unionMediumLargeRedBlueCount);
            }
            else
            {
                providerDatas = session.QueryPSF(fpsf.CombinedSizePsf, new CombinedKey(Constants.Size.Medium),
                                                 fpsf.CombinedColorPsf, new CombinedKey(Color.Blue), (sz, cl) => sz && cl).ToArray();
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumBlueCount), intersectMediumBlueCount);
                providerDatas = session.QueryPSF(new[] { (fpsf.CombinedSizePsf, new[] { new CombinedKey(Constants.Size.Medium) }.AsEnumerable()),
                                                         (fpsf.CombinedColorPsf, new[] { new CombinedKey(Color.Blue) }.AsEnumerable()) }, sz => sz[0] && sz[1]).ToArray();
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumBlueCount), intersectMediumBlueCount);
                // ---
                providerDatas = session.QueryPSF(fpsf.CombinedSizePsf, new CombinedKey(Constants.Size.Medium),
                                                 fpsf.CombinedColorPsf, new CombinedKey(Color.Blue), (sz, cl) => sz || cl).ToArray();
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumBlueCount), unionMediumBlueCount);
                providerDatas = session.QueryPSF(new[] { (fpsf.CombinedSizePsf, new[] { new CombinedKey(Constants.Size.Medium) }.AsEnumerable()),
                                                         (fpsf.CombinedColorPsf, new[] { new CombinedKey(Color.Blue) }.AsEnumerable()) }, sz => sz[0] || sz[1]).ToArray();
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumBlueCount), unionMediumBlueCount);
                // ---
                providerDatas = session.QueryPSF(fpsf.CombinedSizePsf, new CombinedKey(Constants.Size.Medium),
                                                 fpsf.CombinedColorPsf, new CombinedKey(Color.Blue),
                                                 fpsf.CombinedCountBinPsf, new CombinedKey(7), (sz, cl, ct) => sz && cl && ct).ToArray();
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumBlue7Count), intersectMediumBlue7Count);
                providerDatas = session.QueryPSF(new[] {(fpsf.CombinedSizePsf, new [] { new CombinedKey(Constants.Size.Medium) }.AsEnumerable()),
                                                         (fpsf.CombinedColorPsf, new [] { new CombinedKey(Color.Blue) }.AsEnumerable()),
                                                         (fpsf.CombinedCountBinPsf, new [] { new CombinedKey(7) }.AsEnumerable()) }, sz => sz[0] && sz[1] && sz[2]).ToArray();
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumBlue7Count), intersectMediumBlue7Count);
                // ---
                providerDatas = session.QueryPSF(fpsf.CombinedSizePsf, new CombinedKey(Constants.Size.Medium),
                                                 fpsf.CombinedColorPsf, new CombinedKey(Color.Blue),
                                                 fpsf.CombinedCountBinPsf, new CombinedKey(7), (sz, cl, ct) => sz || cl || ct).ToArray();
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumBlue7Count), unionMediumBlue7Count);
                providerDatas = session.QueryPSF(new[] {(fpsf.CombinedSizePsf, new [] { new CombinedKey(Constants.Size.Medium) }.AsEnumerable()),
                                                         (fpsf.CombinedColorPsf, new [] { new CombinedKey(Color.Blue) }.AsEnumerable()),
                                                         (fpsf.CombinedCountBinPsf, new [] { new CombinedKey(7) }.AsEnumerable()) }, sz => sz[0] || sz[1] || sz[2]).ToArray();
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumBlue7Count), unionMediumBlue7Count);
                // ---
                providerDatas = session.QueryPSF(fpsf.CombinedSizePsf, new[] { new CombinedKey(Constants.Size.Medium), new CombinedKey(Constants.Size.Large) }).ToArray();
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumLargeCount), unionMediumLargeCount);
                providerDatas = session.QueryPSF(fpsf.CombinedColorPsf, new[] { new CombinedKey(Color.Blue), new CombinedKey(Color.Red) }).ToArray();
                ok &= VerifyProviderDatas(providerDatas, nameof(unionRedBlueCount), unionRedBlueCount);
                // ---
                providerDatas = session.QueryPSF(fpsf.CombinedSizePsf, new[] { new CombinedKey(Constants.Size.Medium), new CombinedKey(Constants.Size.Large) },
                                                 fpsf.CombinedColorPsf, new[] { new CombinedKey(Color.Blue), new CombinedKey(Color.Red) }, (sz, cl) => sz && cl).ToArray();
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumLargeRedBlueCount), intersectMediumLargeRedBlueCount);
                providerDatas = session.QueryPSF(new[] {(fpsf.CombinedSizePsf, new[] { new CombinedKey(Constants.Size.Medium), new CombinedKey(Constants.Size.Large) }.AsEnumerable()),
                                                         (fpsf.CombinedColorPsf, new[] { new CombinedKey(Color.Blue), new CombinedKey(Color.Red) }.AsEnumerable())}, sz => sz[0] && sz[1]).ToArray();
                ok &= VerifyProviderDatas(providerDatas, nameof(intersectMediumLargeRedBlueCount), intersectMediumLargeRedBlueCount);
                // ---
                providerDatas = session.QueryPSF(fpsf.CombinedSizePsf, new[] { new CombinedKey(Constants.Size.Medium), new CombinedKey(Constants.Size.Large) },
                                                 fpsf.CombinedColorPsf, new[] { new CombinedKey(Color.Blue), new CombinedKey(Color.Red) }, (sz, cl) => sz || cl).ToArray();
                ok &= VerifyProviderDatas(providerDatas, nameof(unionMediumLargeRedBlueCount), unionMediumLargeRedBlueCount);
                providerDatas = session.QueryPSF(new[] {(fpsf.CombinedSizePsf, new[] { new CombinedKey(Constants.Size.Medium), new CombinedKey(Constants.Size.Large) }.AsEnumerable()),
                                                         (fpsf.CombinedColorPsf, new[] { new CombinedKey(Color.Blue), new CombinedKey(Color.Red) }.AsEnumerable())}, sz => sz[0] || sz[1]).ToArray();
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

        internal static bool UpdateSizeByUpsert<TValue, TInput, TOutput, TFunctions, TSerializer>(FPSF<TValue, TInput, TOutput, TFunctions, TSerializer> fpsf)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            Console.WriteLine("Updating Sizes via Upsert");

            using var session = fpsf.fht.NewSession();

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
                // Get the old value and confirm it's as expected.
                ref TValue oldValue = ref providerData.GetValue();
                Debug.Assert(oldValue.SizeInt == (int)Constants.Size.Medium);

                // Clone the old value with updated Size; note that this cannot modify the "ref providerData.GetValue()" in-place as that will bypass PSFs.
                var newValue = new TValue
                {
                    Id = oldValue.Id,
                    SizeInt = (int)Constants.Size.XXLarge, // updated
                    ColorArgb = oldValue.ColorArgb,
                    Count = oldValue.Count
                };

                // Reuse the same key
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

        internal static bool UpdateColorByRMW<TValue, TInput, TOutput, TFunctions, TSerializer>(FPSF<TValue, TInput, TOutput, TFunctions, TSerializer> fpsf)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            Console.WriteLine("Updating Colors via RMW");

            using var session = fpsf.fht.NewSession();

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

        internal static bool UpdateCountByUpsert<TValue, TInput, TOutput, TFunctions, TSerializer>(FPSF<TValue, TInput, TOutput, TFunctions, TSerializer> fpsf)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            Console.WriteLine("Updating Counts via Upsert");

            using var session = fpsf.fht.NewSession();

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
                // Get the old value and confirm it's as expected.
                ref TValue oldValue = ref providerData.GetValue();
                Debug.Assert(CountBinKey.GetBin(oldValue.Count, out int tempBin) && tempBin == bin7);

                // Clone the old value with updated Count; note that this cannot modify the "ref providerData.GetValue()" in-place as that will bypass PSFs.
                var newValue = new TValue
                {
                    Id = oldValue.Id,
                    SizeInt = oldValue.SizeInt,
                    ColorArgb = oldValue.ColorArgb,
                    Count = oldValue.Count + (CountBinKey.LastBin - bin7) * CountBinKey.BinSize // updated
                };
                Debug.Assert(CountBinKey.GetBin(newValue.Count, out tempBin) && tempBin == CountBinKey.LastBin);

                // Reuse the same key
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

        internal static bool Delete<TValue, TInput, TOutput, TFunctions, TSerializer>(FPSF<TValue, TInput, TOutput, TFunctions, TSerializer> fpsf)
            where TValue : IOrders, new()
            where TInput : IInput<TValue>, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, TInput, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            Console.WriteLine("Deleting Colors");

            using var session = fpsf.fht.NewSession();

            FasterKVProviderData<Key, TValue>[] GetColorDatas(Color color)
                => useMultiGroups
                    ? session.QueryPSF(fpsf.ColorPsf, new ColorKey(color)).ToArray()
                    : session.QueryPSF(fpsf.CombinedColorPsf, new CombinedKey(color)).ToArray();

            var redDatas = GetColorDatas(Color.Red);
            Console.WriteLine();
            Console.Write($"Deleting all Reds; initial count {redDatas.Length}");

            var context = new Context<TValue>();
            foreach (var providerData in redDatas)
            {
                // This will call Functions<>.InPlaceUpdater.
                session.Delete(ref providerData.GetKey(), context, serialNo);
            }
            ++serialNo;
            Console.WriteLine();

            redDatas = GetColorDatas(Color.Red);
            var ok = redDatas.Length == 0;
            Console.Write(ok ? "Passed" : "*** Failed *** ");
            Console.WriteLine($": Red {redDatas.Length}");
            return ok;
        }
    }
}
