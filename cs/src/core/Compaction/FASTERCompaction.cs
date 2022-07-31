using System;

namespace FASTER.core
{
    /// <summary>
    /// Compaction methods
    /// </summary>
    public partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="functions">Functions used to manage key-values during compaction</param>
        /// <param name="cf">User provided compaction functions (see <see cref="ICompactionFunctions{Key, Value}"/>).</param>
        /// <param name="input">Input for SingleWriter</param>
        /// <param name="output">Output from SingleWriter; it will be called all records that are moved, before Compact() returns, so the user must supply buffering or process each output completely</param>
        /// <param name="untilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <param name="sessionVariableLengthStructSettings">Session variable length struct settings</param>
        /// <returns>Address until which compaction was done</returns>
        internal long Compact<Input, Output, Context, Functions, CompactionFunctions>(Functions functions, CompactionFunctions cf, ref Input input, ref Output output, long untilAddress, CompactionType compactionType, SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
            where CompactionFunctions : ICompactionFunctions<Key, Value>
        {
            return compactionType switch
            {
                CompactionType.Scan => CompactScan<Input, Output, Context, Functions, CompactionFunctions>(functions, cf, ref input, ref output, untilAddress, sessionVariableLengthStructSettings),
                CompactionType.Lookup => CompactLookup<Input, Output, Context, Functions, CompactionFunctions>(functions, cf, ref input, ref output, untilAddress, sessionVariableLengthStructSettings),
                _ => throw new FasterException("Invalid compaction type"),
            };
        }

        private long CompactLookup<Input, Output, Context, Functions, CompactionFunctions>(Functions functions, CompactionFunctions cf, ref Input input, ref Output output, long untilAddress, SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
            where CompactionFunctions : ICompactionFunctions<Key, Value>
        {
            if (untilAddress > hlog.SafeReadOnlyAddress)
                throw new FasterException("Can compact only until Log.SafeReadOnlyAddress");

            var lf = new LogCompactionFunctions<Key, Value, Input, Output, Context, Functions>(functions);
            using var fhtSession = For(lf).NewSession<LogCompactionFunctions<Key, Value, Input, Output, Context, Functions>>(sessionVariableLengthStructSettings: sessionVariableLengthStructSettings);

            using (var iter1 = Log.Scan(Log.BeginAddress, untilAddress))
            {
                while (iter1.GetNext(out var recordInfo))
                {
                    ref var key = ref iter1.GetKey();
                    ref var value = ref iter1.GetValue();

                    if (!recordInfo.Tombstone && !cf.IsDeleted(ref key, ref value))
                    {
                        OperationStatus copyStatus;
                        ReadOptions readOptions = new() { StopAddress = iter1.NextAddress, };
                        do
                        {
                            long checkedAddress = hlog.SafeReadOnlyAddress;
                            
                            var status = fhtSession.Read(ref key, ref input, ref output, ref readOptions, out var recordMetadata);

                            // For now, we perform each record compaction separately. In order to do this in parallel to maximize IOPS,
                            // we need a new Conditional Upsert API (CopyToTailIfNotExists)
                            if (status.IsPending)
                            {
                                fhtSession.CompletePendingWithOutputs(out var completedOutput, true);
                                try
                                {
                                    if (completedOutput.Next())
                                    {
                                        status = completedOutput.Current.Status;
                                        recordMetadata = completedOutput.Current.RecordMetadata;
                                    }
                                    else
                                        throw new FasterException("Pending Read did not complete during compaction");
                                }
                                finally
                                {
                                    completedOutput.Dispose();
                                }
                            }

                            // Either record was found in future, or we returned NOTFOUND because of a tombstone in future
                            if (status.Found || recordMetadata.Address >= iter1.NextAddress)
                                break;
                            
                            copyStatus = fhtSession.CompactionCopyToTail(ref key, ref input, ref value, ref output, checkedAddress);
                            recordMetadata.RecordInfo.PreviousAddress = checkedAddress;
                        } while (copyStatus == OperationStatus.RECORD_ON_DISK);
                    }

                    // Ensure address is at record boundary
                    untilAddress = iter1.NextAddress;
                }
            }
            Log.ShiftBeginAddress(untilAddress, false);
            return untilAddress;
        }

        private long CompactScan<Input, Output, Context, Functions, CompactionFunctions>(Functions functions, CompactionFunctions cf, ref Input input, ref Output output, long untilAddress, SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
            where CompactionFunctions : ICompactionFunctions<Key, Value>
        {
            if (untilAddress > hlog.SafeReadOnlyAddress)
                throw new FasterException("Can compact only until Log.SafeReadOnlyAddress");

            var originalUntilAddress = untilAddress;

            var lf = new LogCompactionFunctions<Key, Value, Input, Output, Context, Functions>(functions);
            using var fhtSession = For(lf).NewSession<LogCompactionFunctions<Key, Value, Input, Output, Context, Functions>>(sessionVariableLengthStructSettings: sessionVariableLengthStructSettings);

            VariableLengthStructSettings<Key, Value> variableLengthStructSettings = null;
            if (hlog is VariableLengthBlittableAllocator<Key, Value> varLen)
            {
                variableLengthStructSettings = new VariableLengthStructSettings<Key, Value>
                {
                    keyLength = varLen.KeyLength,
                    valueLength = varLen.ValueLength,
                };
            }

            using (var tempKv = new FasterKV<Key, Value>(IndexSize, new LogSettings { LogDevice = new NullDevice(), ObjectLogDevice = new NullDevice() }, comparer: Comparer, variableLengthStructSettings: variableLengthStructSettings, loggerFactory: loggerFactory))
            using (var tempKvSession = tempKv.NewSession<Input, Output, Context, Functions>(functions))
            {
                using (var iter1 = Log.Scan(hlog.BeginAddress, untilAddress))
                {
                    while (iter1.GetNext(out var recordInfo))
                    {
                        ref var key = ref iter1.GetKey();
                        ref var value = ref iter1.GetValue();

                        if (recordInfo.Tombstone || cf.IsDeleted(ref key, ref value))
                        {
                            tempKvSession.Delete(ref key, default, 0);
                        }
                        else
                        {
                            tempKvSession.Upsert(ref key, ref value, default, 0);
                        }
                    }
                    // Ensure address is at record boundary
                    untilAddress = originalUntilAddress = iter1.NextAddress;
                }

                // Scan until SafeReadOnlyAddress
                var scanUntil = hlog.SafeReadOnlyAddress;
                if (untilAddress < scanUntil)
                    LogScanForValidity(ref untilAddress, scanUntil, tempKvSession);

                using var iter3 = tempKv.Log.Scan(tempKv.Log.BeginAddress, tempKv.Log.TailAddress);
                while (iter3.GetNext(out var recordInfo))
                {
                    if (!recordInfo.Tombstone)
                    {
                        OperationStatus copyStatus = default;
                        do
                        {
                            // Try to ensure we have checked all immutable records
                            scanUntil = hlog.SafeReadOnlyAddress;
                            if (untilAddress < scanUntil)
                                LogScanForValidity(ref untilAddress, scanUntil, tempKvSession);

                            // If record is not the latest in memory
                            if (tempKvSession.ContainsKeyInMemory(ref iter3.GetKey(), out long tempKeyAddress).Found)
                            {
                                if (iter3.CurrentAddress != tempKeyAddress)
                                    continue;
                            }
                            else
                            {
                                // Possibly deleted key (once ContainsKeyInMemory is updated to check Tombstones)
                                continue;
                            }
                            // As long as there's no record of the same key whose address is >= untilAddress (scan boundary),
                            // we are safe to copy the old record to the tail.
                            copyStatus = fhtSession.CompactionCopyToTail(ref iter3.GetKey(), ref input, ref iter3.GetValue(), ref output, untilAddress - 1);
                        } while (copyStatus == OperationStatus.RECORD_ON_DISK);
                    }
                }
            }
            Log.ShiftBeginAddress(originalUntilAddress, false);
            return originalUntilAddress;
        }

        private void LogScanForValidity<Input, Output, Context, Functions>(ref long untilAddress, long scanUntil, ClientSession<Key, Value, Input, Output, Context, Functions> tempKvSession)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            using var iter = Log.Scan(untilAddress, scanUntil);
            while (iter.GetNext(out var _))
            {
                ref var k = ref iter.GetKey();
                ref var v = ref iter.GetValue();

                tempKvSession.Delete(ref k, default, 0);
                untilAddress = iter.NextAddress;
            }
        }
    }
}
