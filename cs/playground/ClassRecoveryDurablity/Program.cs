// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading.Tasks;
using FASTER.core;

namespace ClassRecoveryDurablity
{
    class Program
    {
        static bool stop;
        static int deleteWindow = 5;
        static int indexAhead = 10000000;
        static int startDeleteHeight = 20;
        static int addCount = 100;
        static int deletePosition = 20;

        static void Main(string[] args)
        {
            Task.Run(() =>
            {
                Console.WriteLine("Press any key to stop");
                Console.ReadKey();
                stop = true;
            });

            int stopAfterIteration = args.Any() ? int.Parse(args[0]) : 10;

            while (true)
            {
                Filldb(stopAfterIteration);

                if (stop == true)
                    break;
            }
        }

        static void Filldb(int stopAfterIteration = 5)
        {
            Storedb store = new Storedb(@"C:\FasterTest\data");

            Console.WriteLine("call init db");
            var first = store.InitAndRecover();

            var lastblockKey = new Types.StoreKey { tableType = "L", key = new byte[1] { 0 } };
            var lastBlockvalue = new Types.StoreValue();

            if (first)
            {
                var ss = store.db.NewSession();

                lastBlockvalue = new Types.StoreValue { value = BitConverter.GetBytes(0) };
                Types.StoreContext context1 = new Types.StoreContext();
                ss.Upsert(ref lastblockKey, ref lastBlockvalue, context1, 1);

                ss.CompletePending(true);
                ss.Dispose();

                store.Checkpoint();
            }

            TestData(store, 20);

            Task.Run(() =>
            {
                // insert loop
                var session = store.db.NewSession();

                while (stop == false)
                {
                    Types.StoreInput input = new Types.StoreInput();
                    Types.StoreOutput output = new Types.StoreOutput();
                    lastblockKey = new Types.StoreKey { tableType = "L", key = new byte[1] { 0 } };
                    Types.StoreContext context1 = new Types.StoreContext();
                    var blkStatus = session.Read(ref lastblockKey, ref input, ref output, context1, 1);
                    var blockHeight = BitConverter.ToUInt32(output.value.value);
                    blockHeight += 1;

                    var start = blockHeight;
                    var toadd = start * indexAhead;
                    for (long i = toadd; i < toadd + addCount; i++)
                    {
                        var data = Generate(i);

                        var upsertKey = new Types.StoreKey { tableType = "C", key = data.key };
                        var upsertValue = new Types.StoreValue { value = data.data };
                        Types.StoreContext context2 = new Types.StoreContext();
                        var addStatus = session.Upsert(ref upsertKey, ref upsertValue, context2, 1);
                        
                        // Console.WriteLine("add=" + i);

                        if (addStatus != Status.OK)
                            throw new Exception();
                    }

                    if (start > startDeleteHeight)
                    {
                        var todelete = (start - deleteWindow) * indexAhead;

                        for (long i = todelete; i < todelete + addCount; i++)
                        {
                            if (i % deletePosition == 0)
                            {
                                var data = Generate(i);

                                var deteletKey = new Types.StoreKey { tableType = "C", key = data.key };
                                Types.StoreContext context2 = new Types.StoreContext();
                                var deleteStatus = session.Delete(ref deteletKey, context2, 1);
                                // Console.WriteLine("delete=" + i);

                                if (deleteStatus != Status.OK)
                                    throw new Exception();
                            }
                        }
                    }

                    Console.WriteLine(blockHeight + " processed");

                    lastBlockvalue = new Types.StoreValue { value = BitConverter.GetBytes(blockHeight) };
                    lastblockKey = new Types.StoreKey { tableType = "L", key = new byte[1] { 0 } };
                    Types.StoreContext context = new Types.StoreContext();
                    session.Upsert(ref lastblockKey, ref lastBlockvalue, context, 1);
                    session.CompletePending(true);

                    if (blockHeight % 1000 == 0)
                    {
                        session.Refresh();
                        Console.WriteLine(blockHeight + " refresh done" + blockHeight);
                    }

                    if (blockHeight % 5000 == 0)
                    {
                        store.Checkpoint();
                        Console.WriteLine(blockHeight + " checkpoint done" + blockHeight);
                    }

                    if (blockHeight % stopAfterIteration == 0)
                        break;
                }

                session.Dispose();

            }).Wait();

            store.Checkpoint();
            Console.WriteLine("checkpoint done");

            TestData(store, 20);

            Console.WriteLine("call dispose");
            store.Dispose();

        }

        static void TestData(Storedb store, int  count)
        {
            Task.Run(() =>
            {
                var session = store.db.NewSession();

                var lastblockKey = new Types.StoreKey { tableType = "L", key = new byte[1] { 0 } };
                var lastBlockvalue = new Types.StoreValue();

                // test all data up to now.
                Types.StoreInput input = new Types.StoreInput();
                Types.StoreOutput output = new Types.StoreOutput();
                lastblockKey = new Types.StoreKey { tableType = "L", key = new byte[1] { 0 } };
                Types.StoreContext context1 = new Types.StoreContext();
                var blkStatus = session.Read(ref lastblockKey, ref input, ref output, context1, 1);
                var blockHeight = BitConverter.ToUInt32(output.value.value);

                var from = count == -1 ? 1 : blockHeight - count;
                if (from <= 0)
                    return;

                while (from <= blockHeight)
                {
                    var start = from;
                    var toadd = start * indexAhead;
                    var todelete = start * indexAhead;
                    var deleteHeight = from > startDeleteHeight - deleteWindow && from <= blockHeight - deleteWindow;

                    for (long i = toadd, j = todelete; i < toadd + addCount; i++, j++)
                    {
                        if (deleteHeight && (j % deletePosition == 0))
                        {
                            var data = Generate(j);

                            Types.StoreInput input1 = new Types.StoreInput();
                            Types.StoreOutput output1 = new Types.StoreOutput();
                            Types.StoreContext context = new Types.StoreContext();
                            var readKey = new Types.StoreKey { tableType = "C", key = data.key };

                            var deleteStatus = session.Read(ref readKey, ref input1, ref output1, context, 1);
                            //Console.WriteLine("test delete=" + i);

                            if (deleteStatus == Status.PENDING)
                            {
                                session.CompletePending(true);
                                context.FinalizeRead(ref deleteStatus, ref output1);
                            }

                            if (deleteStatus != Status.NOTFOUND)
                                throw new Exception();
                        }
                        else
                        {
                            var data = Generate(i);

                            Types.StoreInput input1 = new Types.StoreInput();
                            Types.StoreOutput output1 = new Types.StoreOutput();
                            Types.StoreContext context = new Types.StoreContext();
                            var readKey = new Types.StoreKey { tableType = "C", key = data.key };
                            var addStatus = session.Read(ref readKey, ref input1, ref output1, context, 1);
                            //Console.WriteLine("test add=" + i);

                            if (addStatus == Status.PENDING)
                            {
                                session.CompletePending(true);
                                context.FinalizeRead(ref addStatus, ref output1);
                            }

                            if (addStatus != Status.OK)
                                throw new Exception();

                            if (output1.value.value.SequenceEqual(data.data) == false)
                                throw new Exception();
                        }
                    }

                    Console.WriteLine(from + " tested");

                    from++;
                }

                session.Dispose();

            }).Wait();
        }

        static (byte[] key, byte[] data) Generate(long i)
        {
            // Generate data deterministically 
            byte[] key = BitConverter.GetBytes(i * int.MaxValue);

            byte[] data = i % 2 == 0 ?
                System.Text.Encoding.UTF8.GetBytes("00000000000000000000000000000000000000000000000000000000000000000000000000000000000" + i) :
                System.Text.Encoding.UTF8.GetBytes("11111111111111111111111111111111111111111111111111111111111111111111" + i);

            return (key, data);
        }

        public static byte[] Hash256(byte[] byteContents)
        {
            using var hash = new System.Security.Cryptography.SHA256CryptoServiceProvider();
            return hash.ComputeHash(byteContents);
        }
    }
}

