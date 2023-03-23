using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;

namespace FASTER.libdpr
{
    /// <summary>
    /// Iterator for scanning a DARQ
    /// </summary>
    public class DarqScanIterator : IDisposable
    {
        private FasterLogScanIterator iterator;
        private long replayEnd;
        private Queue<(long, long, byte[])> stateMessagesToReplay;
        private Dictionary<long, long> replayMessages;
        private bool disposed = false;
        private byte[] reusedReadBuffer;
        private GCHandle? handle = null;
        
        internal DarqScanIterator(FasterLog log, long replayEnd, bool speculative, bool replay = true)
        {
            iterator = log.Scan(0, long.MaxValue, scanUncommitted: speculative);
            stateMessagesToReplay = new Queue<(long, long, byte[])>();
            replayMessages = new Dictionary<long, long>();
            this.replayEnd = replayEnd;
            if (replay)
                ScanOnRecovery();
        }

        /// <inheritdoc/>>
        public void Dispose()
        {
            disposed = true;
            iterator.Dispose();
        }

        private unsafe void ScanOnRecovery()
        {
            while (true)
            {
                while (iterator.UnsafeGetNext(out var entry, out var length, out var currentAddress,
                           out var nextAddress))
                {
                    // Should not be inclusive -- replay end is the start address of the last completion record in stepped
                    if (currentAddress > replayEnd)
                    {
                        Console.WriteLine(
                            $"Current addr {currentAddress} is beyond replay end {replayEnd}, finishing processor recovery...");
                        break;
                    }

                    switch (*(DarqMessageType *) entry)
                    {
                        case DarqMessageType.OUT:
                            break;
                        case DarqMessageType.SELF:
                            stateMessagesToReplay.Enqueue((currentAddress, nextAddress,
                                new Span<byte>(entry, length).ToArray()));
                            break;
                        case DarqMessageType.IN:
                            replayMessages.Add(currentAddress, length);
                            break;
                        case DarqMessageType.COMPLETION:
                            var completed = (long*)(entry + sizeof(DarqMessageType));
                            while (completed < entry + length)
                            {
                                var completedLsn = *completed++;
                                replayMessages.Remove(completedLsn);
                            }
                            break;
                        case DarqMessageType.CHECKPOINT:
                            stateMessagesToReplay.Clear();
                            break;
                        default:
                            throw new NotImplementedException();
                    }
                }

                if (iterator.NextAddress >= replayEnd) break;
                iterator.WaitAsync().AsTask().GetAwaiter().GetResult();
            }
            iterator.Reset();
        }

        /// <summary>
        /// Scan the next entry in DARQ. If successful, must be followed by a UnsafeRelease call to release any
        /// resources held in-place for unsafe consumption.
        /// </summary>
        /// <param name="entry"> pointer to the start of next entry body</param>
        /// <param name="entryLength">length of the next entry</param>
        /// <param name="currentAddress">address of the entry on DARQ (lsn)</param>
        /// <param name="nextAddress"> lower bound of the address of the next entry on DARQ</param>
        /// <param name="type"> type of entry </param>
        /// <returns>whether a next entry is available at this moment</returns>
        public unsafe bool UnsafeGetNext(out byte* entry, out int entryLength, out long currentAddress,
            out long nextAddress, out DarqMessageType type)
        {
            if (handle.HasValue)
                throw new FasterException("Trying to get next without release previous");
            type = default;
            // Try to replay state messages first
            if (stateMessagesToReplay.Count != 0)
            {
                byte[] bytes;
                (currentAddress, nextAddress, bytes) = stateMessagesToReplay.Dequeue();
                handle = GCHandle.Alloc(bytes, GCHandleType.Pinned);
                type = DarqMessageType.SELF;
                entry = (byte*) handle.Value.AddrOfPinnedObject();
                entryLength = bytes.Length;
                return true;
            }

            while (true)
            {
                if (!iterator.UnsafeGetNext(out entry, out entryLength, out currentAddress, out nextAddress))
                    return false;

                type = (DarqMessageType) (*entry);

                if (currentAddress <= replayEnd)
                {
                    switch (type)
                    {
                        case DarqMessageType.IN:
                            if (replayMessages.Remove(currentAddress)) break;
                            // skip messages we have identified to have been skipped
                            iterator.UnsafeRelease();
                            continue;
                        case DarqMessageType.OUT:
                            break;
                        // These have already been replayed or should not be replayed
                        case DarqMessageType.SELF:
                        case DarqMessageType.COMPLETION:
                        case DarqMessageType.CHECKPOINT:
                            iterator.UnsafeRelease();
                            continue;
                        default:
                            throw new FasterException("Unexpected entry type");
                    }
                }

                // Skip header byte
                entry += sizeof(byte);
                entryLength -= 1;
                return true;
            }
        }

        /// <summary>
        /// Releases resources held from a previous successful UnsafeGetNext call 
        /// </summary>
        public void UnsafeRelease()
        {
            if (handle.HasValue)
            {
                handle.Value.Free();
                handle = null;
            }
            else
                iterator.UnsafeRelease();
        }

        /// <summary>
        /// Wait until the next entry is available or when no more entries will be available
        /// </summary>
        /// <param name="token">cancellation token</param>
        /// <returns> task for the availability of next entry </returns>
        public ValueTask<bool> WaitAsync(CancellationToken token = default)
        {
            return iterator.WaitAsync(token);
        }
    }
}