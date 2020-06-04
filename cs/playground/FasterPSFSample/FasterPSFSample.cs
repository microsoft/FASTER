// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

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
        private static int blueCount;
        private static int mediumCount;
        private static int mediumBlueCount;

        internal static Dictionary<Key, IOrders> keyDict = new Dictionary<Key, IOrders>();

        private static int nextId = 1000000000;

        internal static int IPUColor;

        static void Main(string[] argv)
        {
            if (!ParseArgs(argv))
                return;

            if (useObjectValue)  // TODO add VarLenValue
                RunSample<ObjectOrders, Output<ObjectOrders>, ObjectOrders.Functions, ObjectOrders.Serializer>();
            else
                RunSample<BlittableOrders, Output<BlittableOrders>, BlittableOrders.Functions, NoSerializer>();
            return;
        }

        internal static void RunSample<TValue, TOutput, TFunctions, TSerializer>()
            where TValue : IOrders, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, Input, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            var fpsf = new FPSF<TValue, TOutput, TFunctions, TSerializer>(useObjectValue, useReadCache: true);
            try
            {
                RunUpserts(fpsf);
                RunReads(fpsf);
                var ok = QueryPSFs(fpsf)
                        && UpdateByUpsert(fpsf)
                        && UpdateByRMW(fpsf)
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

            Console.WriteLine("Press <ENTER> to end");
            Console.ReadLine();
        }

        internal static void RunUpserts<TValue, TOutput, TFunctions, TSerializer>(FPSF<TValue, TOutput, TFunctions, TSerializer> fpsf)
            where TValue : IOrders, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, Input, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine("Writing keys from 0 to {0} to FASTER", UpsertCount);

            var rng = new Random(13);
            using var session = fpsf.fht.NewSession();
            var context = new Context<TValue>();

            for (int i = 0; i < UpsertCount; i++)
            {
                // Leave the last value unassigned from each category (we'll use it to update later
                var key = new Key(Interlocked.Increment(ref nextId) - 1);
                var value = new TValue
                {
                    SizeInt = rng.Next((int)Constants.Size.NumSizes - 1),
                    ColorArgb = Constants.Colors[rng.Next(Constants.Colors.Length - 1)].ToArgb(),
                    NumSold = rng.Next(OrdersBinKey.MaxOrders - 1)
                };
                keyDict[key] = value;
                if (value.ColorArgb == Color.Blue.ToArgb())
                {
                    ++blueCount;
                    if (value.SizeInt == (int)Constants.Size.Medium)
                        ++mediumBlueCount;
                }
                if (value.SizeInt == (int)Constants.Size.Medium)
                    ++mediumCount;
                session.Upsert(ref key, ref value, context, 0);
            }

            Console.WriteLine($"Upserted {UpsertCount} elements");
        }

        internal static void RunReads<TValue, TOutput, TFunctions, TSerializer>(FPSF<TValue, TOutput, TFunctions, TSerializer> fpsf)
            where TValue : IOrders, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, Input, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine("Reading {0} random keys from FASTER", UpsertCount);

            var rng = new Random(0);
            int statusPending = 0;
            var output = new TOutput();
            var input = default(Input);
            var context = new Context<TValue>();
            var readCount = UpsertCount * 2;

            var keys = keyDict.Keys.ToArray();

            using var session = fpsf.fht.NewSession();
            for (int i = 0; i < UpsertCount; i++)
            {
                var key = keys[rng.Next(keys.Length)];
                var status = session.Read(ref key, ref input, ref output, context, 0);

                if (status == Status.OK && output.Value.MemberTuple != keyDict[key].MemberTuple)
                    throw new Exception($"Error: Value does not match key in {nameof(RunReads)}");
            }

            session.CompletePending(true);
            Console.WriteLine($"Read {readCount} elements with {++statusPending} Pending");
        }

        const string indent = "    ";

        internal static bool QueryPSFs<TValue, TOutput, TFunctions, TSerializer>(FPSF<TValue, TOutput, TFunctions, TSerializer> fpsf)
            where TValue : IOrders, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, Input, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            Console.WriteLine("Querying PSFs from FASTER", UpsertCount);

            using var session = fpsf.fht.NewSession();

            var actualCount = 0;
            var ok = true;
            Console.WriteLine();
            Console.WriteLine("No join op; all Blues: ");
            foreach (var providerData in session.QueryPSF(fpsf.ColorPsf, new ColorKey(Color.Blue)))
            {
                ++actualCount;
                ref TValue value = ref providerData.GetValue();
                if (verbose)
                    Console.WriteLine(indent + value);
            }
            ok &= blueCount == actualCount;
            Console.WriteLine(blueCount == actualCount
                              ? $"Blue Passed: expected == actual ({blueCount})"
                              : $"Blue Failed: expected ({blueCount}) != actual ({actualCount})");

            actualCount = 0;
            Console.WriteLine();
            Console.WriteLine("No join op; all Mediums: ");
            foreach (var providerData in session.QueryPSF(fpsf.SizePsf, new SizeKey(Constants.Size.Medium)))
            {
                ++actualCount;
                ref TValue value = ref providerData.GetValue();
                if (verbose)
                    Console.WriteLine(indent + value);
            }
            ok &= mediumCount == actualCount;
            Console.WriteLine(mediumCount == actualCount
                              ? $"Medium Passed: expected == actual ({mediumCount})"
                              : $"Medium Failed: expected ({mediumCount}) != actual ({actualCount})");
            return ok;
        }

        internal static bool UpdateByUpsert<TValue, TOutput, TFunctions, TSerializer>(FPSF<TValue, TOutput, TFunctions, TSerializer> fpsf)
            where TValue : IOrders, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, Input, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            Console.WriteLine("Updating Sizes");

            using var session = fpsf.fht.NewSession();
            var xxlDatas = session.QueryPSF(fpsf.SizePsf, new SizeKey(Constants.Size.XXLarge)).ToArray();
            var mediumDatas = session.QueryPSF(fpsf.SizePsf, new SizeKey(Constants.Size.Medium)).ToArray();
            var expected = mediumDatas.Length;
            Console.WriteLine();
            Console.Write($"Changing all Medium to XXLarge; initial counts Medium {mediumDatas.Length}, XXLarge {xxlDatas.Length}");

            var context = new Context<TValue>();
            foreach (var providerData in mediumDatas)
            {
                // Update the value
                ref TValue value = ref providerData.GetValue();
                Debug.Assert(value.SizeInt == (int)Constants.Size.Medium);
                value.SizeInt = (int)Constants.Size.XXLarge;

                // Reuse the same key
                session.Upsert(ref providerData.GetKey(), ref value, context, 2);
            }
            Console.WriteLine();

            xxlDatas = session.QueryPSF(fpsf.SizePsf, new SizeKey(Constants.Size.XXLarge)).ToArray();
            mediumDatas = session.QueryPSF(fpsf.SizePsf, new SizeKey(Constants.Size.Medium)).ToArray();
            bool ok = xxlDatas.Length == expected && mediumDatas.Length == 0;
            Console.Write(ok ? "Passed" : "*** Failed *** ");
            Console.WriteLine($": Medium {mediumDatas.Length}, XXLarge {xxlDatas.Length}");
            return ok;
        }

        internal static bool UpdateByRMW<TValue, TOutput, TFunctions, TSerializer>(FPSF<TValue, TOutput, TFunctions, TSerializer> fpsf)
            where TValue : IOrders, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, Input, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            Console.WriteLine("Updating Colors");

            using var session = fpsf.fht.NewSession();
            var purpleDatas = session.QueryPSF(fpsf.ColorPsf, new ColorKey(Color.Purple)).ToArray();
            var blueDatas = session.QueryPSF(fpsf.ColorPsf, new ColorKey(Color.Blue)).ToArray();
            var expected = blueDatas.Length;
            Console.WriteLine();
            Console.Write($"Changing all Blue to Purple; initial counts Blue {blueDatas.Length}, Purple {purpleDatas.Length}");

            IPUColor = Color.Purple.ToArgb();
            var context = new Context<TValue>();
            var input = default(Input);
            foreach (var providerData in blueDatas)
            {
                // This will call Functions<>.InPlaceUpdater.
                session.RMW(ref providerData.GetKey(), ref input, context, 3);
            }
            Console.WriteLine();

            purpleDatas = session.QueryPSF(fpsf.ColorPsf, new ColorKey(Color.Purple)).ToArray();
            blueDatas = session.QueryPSF(fpsf.ColorPsf, new ColorKey(Color.Blue)).ToArray();
            bool ok = purpleDatas.Length == expected && blueDatas.Length == 0;
            Console.Write(ok ? "Passed" : "*** Failed *** ");
            Console.WriteLine($": Blue {blueDatas.Length}, Purple {purpleDatas.Length}");
            return ok;
        }

        internal static bool Delete<TValue, TOutput, TFunctions, TSerializer>(FPSF<TValue, TOutput, TFunctions, TSerializer> fpsf)
            where TValue : IOrders, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, Input, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine();
            Console.WriteLine("Deleting Colors");

            using var session = fpsf.fht.NewSession();
            var redDatas = session.QueryPSF(fpsf.ColorPsf, new ColorKey(Color.Red)).ToArray();
            Console.WriteLine();
            Console.Write($"Deleting all Reds; initial count {redDatas.Length}");

            var context = new Context<TValue>();
            foreach (var providerData in redDatas)
            {
                // This will call Functions<>.InPlaceUpdater.
                session.Delete(ref providerData.GetKey(), context, 4);
            }
            Console.WriteLine();

            redDatas = session.QueryPSF(fpsf.ColorPsf, new ColorKey(Color.Red)).ToArray();
            var ok = redDatas.Length == 0;
            Console.Write(ok ? "Passed" : "*** Failed *** ");
            Console.WriteLine($": Red {redDatas.Length}");
            return ok;
        }
    }
}
