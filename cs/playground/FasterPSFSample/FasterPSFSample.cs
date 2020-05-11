// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Diagnostics;

namespace FasterPSFSample
{
    public partial class FasterPSFSample
    {
        private const int UpsertCount = 100;

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
                RunPSFs(fpsf);
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
            using (var session = fpsf.fht.NewSession())
            {
                var context = new Context<TValue>();

                for (int i = 0; i < UpsertCount; i++)
                {
                    var key = new Key((Constants.Size)rng.Next((int)Constants.Size.NumSizes),
                                      Constants.Colors[rng.Next((int)Constants.Colors.Length)],
                                      rng.Next(OrdersBinKey.MaxOrders));
                    var value = new TValue { Size = key.Size, Color = key.Color, NumSold = key.NumSold };
                    session.Upsert(ref key, ref value, context, 0);
                }
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

            using (var session = fpsf.fht.NewSession())
            {
                for (int i = 0; i < UpsertCount; i++)
                {
                    var key = new Key((Constants.Size)rng.Next((int)Constants.Size.NumSizes),
                                      Constants.Colors[rng.Next((int)Constants.Colors.Length)],
                                      rng.Next(OrdersBinKey.MaxOrders));
                    var status = session.Read(ref key, ref input, ref output, context, 0);

                    if (status == Status.OK && output.Value.MemberTuple != key.MemberTuple)
                        throw new Exception($"Error: Value does not match key in {nameof(RunReads)}");
                }

                session.CompletePending(true);
            }
            Console.WriteLine($"Read {readCount} elements with {++statusPending} Pending");
        }

        const string indent = "    ";

        internal static void RunPSFs<TValue, TOutput, TFunctions, TSerializer>(FPSF<TValue, TOutput, TFunctions, TSerializer> fpsf)
            where TValue : IOrders, new()
            where TOutput : IOutput<TValue>, new()
            where TFunctions : IFunctions<Key, TValue, Input, TOutput, Context<TValue>>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Console.WriteLine("Querying PSFs from FASTER", UpsertCount);

            using (var session = fpsf.fht.NewSession())
            {
                Console.Write("No join op; all Mediums: ");
                foreach (var providerData in session.QueryPSF(fpsf.SizePsf, new SizeKey(Constants.Size.Medium)))
                {
                    Console.WriteLine();
                    ref TValue value = ref providerData.GetValue();
                    Console.Write(indent + value);
                }
                Console.WriteLine();
            }
        }
    }
}
