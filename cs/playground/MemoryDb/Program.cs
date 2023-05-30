using FASTER.core;
using System.Buffers.Binary;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System;

namespace ConsoleApp72;

using MySession = ClientSession<Memory<byte>, Memory<byte>, Memory<byte>, (IMemoryOwner<byte>, int), Empty, UserDataFunctions<Memory<byte>>>;

internal class Program
{
    static async Task Main()
    {
        var sourceDbPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "source");

        WriteSimpleSourceDb(sourceDbPath);

        for (var i = 0; i < 100; i++)
        {
            var dbPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, $"faster-{i}");
            try
            {
                await Write(sourceDbPath, dbPath);
            }
            finally
            {
                try
                {
                    if (Directory.Exists(dbPath))
                        Directory.Delete(dbPath, true);
                }
                catch
                {
                    // ignore
                }
            }
        }
    }

    private static readonly Random random = new();

    private static void WriteSimpleSourceDb(string sourceDbPath)
    {
        Console.WriteLine($"Source: {sourceDbPath}");

        if (Directory.Exists(sourceDbPath))
            Directory.Delete(sourceDbPath, true);

        using var fasterLogSettings = new FasterLogSettings(sourceDbPath);
        using var fasterLog = new FasterLog(fasterLogSettings);

        for (var index = 0; index < 500_000; index++)
        {
            using var valueBuffer = MemoryPool<byte>.Shared.Rent(1024);

            random.NextBytes(valueBuffer.Memory.Span.Slice(0, 1024));

            fasterLog.Enqueue(valueBuffer.Memory.Span.Slice(0, 1024));
        }

        fasterLog.Commit(true);
    }

    private const int SecondaryIndexCnt = 5;

    private static async Task Write(string sourceDbPath, string dbPath)
    {
        Console.WriteLine(dbPath);

        if (Directory.Exists(dbPath))
            Directory.Delete(dbPath, true);

        var mainDbPath = Path.Combine(dbPath, "main");

        var tempIndexDbDirectory = Path.Combine(dbPath, "TempIndex");

        using var mainLogDevice = Devices.CreateLogDevice(mainDbPath);
        using var mainKvSettings = new FasterKVSettings<Memory<byte>, Memory<byte>> { LogDevice = mainLogDevice, PageSize = 1 * 1024 * 1024, SegmentSize = 32 * 1024 * 1024, CheckpointDir = mainDbPath };
        using var mainKv = new FasterKV<Memory<byte>, Memory<byte>>(mainKvSettings);

        var indexDevices = new List<IDevice>();
        var indexSettings = new List<FasterKVSettings<Memory<byte>, Memory<byte>>>();
        var indexKvs = new List<FasterKV<Memory<byte>, Memory<byte>>>();

        for (var i = 0; i < SecondaryIndexCnt; i++)
        {
            var indexPath = Path.Combine(tempIndexDbDirectory, $"i_{i}");

            var indexLogDevice = Devices.CreateLogDevice(indexPath);
            var indexKvSettings = new FasterKVSettings<Memory<byte>, Memory<byte>> { LogDevice = indexLogDevice, PageSize = 1 * 1024 * 1024, SegmentSize = 32 * 1024 * 1024, CheckpointDir = indexPath };
            var indexKv = new FasterKV<Memory<byte>, Memory<byte>>(indexKvSettings);

            indexDevices.Add(indexLogDevice);
            indexSettings.Add(indexKvSettings);
            indexKvs.Add(indexKv);
        }

        {
            using var mainKvSession = mainKv.For(UserDataFunctions<Memory<byte>>.Instance).NewSession<UserDataFunctions<Memory<byte>>>();

            var indexSessions = new List<MySession>();

            foreach (var indexKv in indexKvs)
            {
                indexSessions.Add(indexKv.For(UserDataFunctions<Memory<byte>>.Instance).NewSession<UserDataFunctions<Memory<byte>>>());
            }

            using var sourceLogSettings = new FasterLogSettings(sourceDbPath) { ReadOnlyMode = true };
            using var sourceLog = new FasterLog(sourceLogSettings);

            var recordCounter = 0;

            using var sourceIterator = sourceLog.Scan(sourceLog.BeginAddress, long.MaxValue);

            while (sourceIterator.GetNext(MemoryPool<byte>.Shared, out var memoryOwner, out var wholeLength, out var address))
            {
                recordCounter++;

                using var mo = memoryOwner;

                var wholeValue = memoryOwner.Memory.Slice(0, wholeLength);

                var primaryKey = address;

                using var primaryKeyBuffer = MemoryPool<byte>.Shared.Rent(8);

                BinaryPrimitives.TryWriteInt64BigEndian(primaryKeyBuffer.Memory.Span.Slice(0, 8), primaryKey);

                var primaryKeyContent = primaryKeyBuffer.Memory.Slice(0, 8);

                var secondaryKeys = new List<byte[]>();

                for (var i = 0; i < SecondaryIndexCnt; i++)
                {
                    secondaryKeys.Add(memoryOwner.Memory.Slice(i * 4, 4).ToArray());
                }

                if (TryRead(mainKvSession, primaryKeyContent, out var mainValue))
                {
                    using (mainValue.MemoryOwner)
                    {
                    }
                }

                Write(mainKvSession, primaryKeyContent, wholeValue);

                for (var i = 0; i < SecondaryIndexCnt; i++)
                {
                    var secondaryKeyContent = secondaryKeys[i].AsMemory();

                    if (TryRead(indexSessions[i], secondaryKeyContent, out var readItem))
                    {
                        using (readItem.MemoryOwner)
                        {
                        }
                    }

                    Write(indexSessions[i], secondaryKeyContent, primaryKeyContent);
                }

                if (recordCounter % 100_000 == 0)
                {
                    Console.WriteLine($"{recordCounter}...");
                }
            }

            Console.WriteLine($"{recordCounter} done");

            foreach (var indexSession in indexSessions) indexSession.Dispose();

            foreach (var indexKv in indexKvs)
            {
                await indexKv.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver, true);
            }
        }

        await mainKv.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver, true);

        foreach (var indexKv in indexKvs) indexKv.Dispose();
        foreach (var indexSetting in indexSettings) indexSetting.Dispose();
        foreach (var indexDevice in indexDevices) indexDevice.Dispose();
    }

    private static bool TryRead<TKey>(ClientSession<TKey, Memory<byte>, Memory<byte>, (IMemoryOwner<byte>, int), Empty, UserDataFunctions<TKey>> session, TKey key, out (IMemoryOwner<byte> MemoryOwner, int Length) value)
    {
        var (status, output) = session.Read(key);

        if (status.Found)
        {
            value = output;
            return true;
        }

        if (status.IsPending && session.CompletePendingWithOutputs(out var outputs, true))
        {
            using (outputs)
            {
                if (outputs.Next())
                {
                    value = outputs.Current.Output;
                    return true;
                }
            }
        }

        value = default;
        return false;
    }

    private static void Write<TKey>(ClientSession<TKey, Memory<byte>, Memory<byte>, (IMemoryOwner<byte>, int), Empty, UserDataFunctions<TKey>> session, TKey key, Memory<byte> value)
    {
        var status = session.Upsert(key, value);

        if (status.IsPending)
        {
            session.CompletePending(true);
        }
    }
}

internal sealed class UserDataFunctions<TKey> : MemoryFunctions<TKey, byte, Empty>
{

    private UserDataFunctions()
    {
    }

    public static readonly UserDataFunctions<TKey> Instance = new();

}