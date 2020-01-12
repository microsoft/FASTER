using System;
using System.Linq;
using System.Threading.Tasks;
using FASTER.core;

namespace ClassRecoveryDurablity
{
    class Program
    {
        static bool stop;

        static void Main(string[] args)
        {
            Task.Run(() =>
            {
                Console.WriteLine("Press any key to stop");
                Console.ReadKey();
                stop = true;
            });

            int stopAfterIteration = args.Any() ? int.Parse(args[0]) : 5;

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

            TestData(store);

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
                    var toadd = start * 10000000;
                    for (long i = toadd; i < toadd + 1000; i++)
                    {
                        var data = Generate(i);

                        var upsertKey = new Types.StoreKey { tableType = "C", key = data.key };
                        var upsertValue = new Types.StoreValue { value = data.data };
                        Types.StoreContext context2 = new Types.StoreContext();
                        var addStatus = session.Upsert(ref upsertKey, ref upsertValue, context2, 1);

                        if (addStatus != Status.OK)
                            throw new Exception();
                    }

                    if (blockHeight > 150)
                    {
                        toadd = (start - 5) * 10000000;

                        for (long i = toadd; i < toadd + 1000; i++)
                        {
                            if (i % 100 == 0)
                            {
                                var data = Generate(i);

                                var deteletKey = new Types.StoreKey { tableType = "C", key = data.key };
                                Types.StoreContext context2 = new Types.StoreContext();
                                var deleteStatus = session.Delete(ref deteletKey, context2, 1);

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

            TestData(store);

            Console.WriteLine("call dispose");
            store.Dispose();

        }

        static void TestData(Storedb store)
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

                var from = 1;

                while (from <= blockHeight)
                {
                    var start = from;
                    var toadd = start * 10000000;
                    for (long i = toadd; i < toadd + 1000; i++)
                    {
                        var data = Generate(i);

                        Types.StoreInput input1 = new Types.StoreInput();
                        Types.StoreOutput output1 = new Types.StoreOutput();
                        Types.StoreContext context = new Types.StoreContext();
                        var readKey = new Types.StoreKey { tableType = "C", key = data.key };
                        var addStatus = session.Read(ref readKey, ref input1, ref output1, context, 1);

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

                    if (from > 150)
                    {
                        toadd = (start - 5) * 10000000;

                        for (long i = toadd; i < toadd + 1000; i++)
                        {
                            if (i % 100 == 0)
                            {
                                var data = Generate(i);

                                Types.StoreInput input1 = new Types.StoreInput();
                                Types.StoreOutput output1 = new Types.StoreOutput();
                                Types.StoreContext context = new Types.StoreContext();
                                var readKey = new Types.StoreKey { tableType = "C", key = data.key };

                                var deleteStatus = session.Read(ref readKey, ref input1, ref output1, context, 1);

                                if (deleteStatus == Status.PENDING)
                                {
                                    session.CompletePending(true);
                                    context.FinalizeRead(ref deleteStatus, ref output1);
                                }

                                if (deleteStatus != Status.NOTFOUND)
                                    throw new Exception();
                            }
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
            byte[] key = BitConverter.GetBytes(i * int.MaxValue);
            byte[] data = System.Text.Encoding.UTF8.GetBytes("00000000000000000000000000000000000000000000000000000000000000000000000000000000000" + i);

            return (key, data);
        }

        public static byte[] Hash256(byte[] byteContents)
        {
            var hash = new System.Security.Cryptography.SHA256CryptoServiceProvider();
            byte[] hased = hash.ComputeHash(byteContents);
            return hased;
        }
    }
}

