using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FASTER.common;
using FASTER.darq;
using FASTER.libdpr;

namespace FASTER.server
{
    internal sealed class DarqSubscriptionSession : ServerSessionBase
    {
        readonly HeaderReaderWriter hrw;
        int readHead;
        int seqNo, msgnum, start;
        private Darq dprServer;
        private ManualResetEventSlim terminationStart, terminationComplete;
        private Thread pushThread;
        private unsafe byte* dcurr, dend;
        private unsafe int *dprResponseOffset;
        // TODO(Tianyu): Hacky
        private static int MAX_BATCH_SIZE = 1 << 10;
        // TODO(Tianyu): Hacky
        private byte[] tempBuffer = new byte[1 << 12];


        public DarqSubscriptionSession(INetworkSender networkSender, Darq dprServer) : base(networkSender)
        {
            this.dprServer = dprServer;
        }

        private unsafe bool TryReadMessages(byte* buf, out int offset)
        {
            offset = default;

            var bytesAvailable = bytesRead - readHead;
            // Need to at least have read off of size field on the message
            if (bytesAvailable < sizeof(int)) return false;

            // MSB is 1 to indicate binary protocol
            var size = -(*(int*) buf);

            // Not all of the message has arrived
            if (bytesAvailable < size + sizeof(int)) return false;
            offset = readHead + sizeof(int);

            // Consume this message and the header
            readHead += size + sizeof(int);
            return true;
        }
        
        public override void Dispose()
        {
            terminationStart.Set();
            terminationComplete.Wait();
            pushThread?.Join();
            base.Dispose();
        }

        public override unsafe int TryConsumeMessages(byte* req_buf, int bytesReceived)
        {
            bytesRead = bytesReceived;
            readHead = 0;
            while (TryReadMessages(req_buf, out var offset))
                ProcessBatch(req_buf, offset);
            return readHead;
        }

        private unsafe void ProcessBatch(byte* buf, int offset)
        {
            var src = buf + offset;
            ref var header = ref Unsafe.AsRef<BatchHeader>(src);
            var num = header.NumMessages;
            src += BatchHeader.Size;
            for (msgnum = 0; msgnum < num; msgnum++)
            {
                var m = (MessageType) (*src++);
                switch (m)
                {
                    case MessageType.DarqStartPush:
                    {
                        var speculative = (*src++) == 1;
                        var t = new ManualResetEventSlim();
                        if (Interlocked.CompareExchange(ref terminationStart, t, null) == null)
                        {
                            terminationComplete = new ManualResetEventSlim();
                            StartPushEntries(speculative);
                        }
                        break;
                    }
                    default:
                        throw new NotImplementedException();
                }
            }
        }

        private void StartPushEntries(bool speculative)
        {
            pushThread = new Thread(async () =>
            {
                using var it = dprServer.StartScan(speculative);
                while (!terminationStart.IsSet)
                {
                    ResetSendBuffer();
                    while (TrySendEntry(it) && msgnum < MAX_BATCH_SIZE) {}
                    if (msgnum != 0)
                        SendCurrentBuffer();
                    else
                        dprServer.FinishProcessing();

                    if (terminationStart.IsSet) break;
                    
                    // dprServer.StateObject().RefreshSafeReadTail();
                    var iteratorWait = it.WaitAsync().AsTask();
                    if (await Task.WhenAny(iteratorWait, Task.Delay(10)) == iteratorWait)
                    {
                        // No more entries, can signal finished and return 
                        if (!iteratorWait.Result) break;
                    }
                    // Otherwise, just continue looping
                }

                terminationComplete.Set();
            });
            pushThread.Start();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void ResetSendBuffer()
        {
            msgnum = 0;
            networkSender.GetResponseObject();
            var d = networkSender.GetResponseObjectHead();
            dend = networkSender.GetResponseObjectTail();
            dcurr = d + sizeof(int); // reserve space for size
            dcurr += BatchHeader.Size;
            dprResponseOffset = (int*) dcurr;
            dcurr += sizeof(int);
            dprServer.BeginProcessing();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void SendCurrentBuffer()
        {
            // Finish composing DPR batch
            *dprResponseOffset = (int) (dcurr - (byte*) dprResponseOffset) - sizeof(int);
            // reserve a size field for DPR header;
            var dprHeaderSizeField = (int*) dcurr;
            dcurr += sizeof(int);
            dprServer.FinishProcessingAndSend(new Span<byte>(dcurr, (int) (dend - dcurr)));
            dcurr += DprBatchHeader.FixedLenSize;
            // Write size
            *dprHeaderSizeField = DprBatchHeader.FixedLenSize;
            Debug.Assert(dcurr < dend);

            var d = networkSender.GetResponseObjectHead();
            var dstart = d + sizeof(int);
            ((BatchHeader*) dstart)->SetNumMessagesProtocol(msgnum - start, WireFormat.DarqSubscribe);
            ((BatchHeader*) dstart)->SeqNo = seqNo++;
            int payloadSize = (int) (dcurr - d);
            // Set packet size in header
            *(int*) d = -(payloadSize - sizeof(int));
            networkSender.SendResponse(0, payloadSize);
        }

        // TODO(Tianyu): More efficiently batch entries together once we figure out how to get size computation to work
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe bool TrySendEntry(DarqScanIterator it)
        {
            if (it.UnsafeGetNext(out var entry, out var entryLength, out var lsn, out var nextLsn, out var type))
            {
                
                if (type != DarqMessageType.IN && type != DarqMessageType.SELF)
                {
                    it.UnsafeRelease();
                    return true;
                }

                var spaceRequired = 2 * sizeof(long) + sizeof(int) + sizeof(MessageType) + entryLength;
                // TODO(Tianyu): hacky --- we are not supposed to know about header size details as that's implementation specific
                var dprHeaderSpace = (1 + msgnum) * sizeof(long) + DprBatchHeader.FixedLenSize + sizeof(int);
                
                if (dend - dcurr < spaceRequired + dprHeaderSpace)
                {
                    // TODO(Tianyu): Not very elegant --- either don't use the unsafe interface here or do something else?
                    // Must release iterator epoch as we may block on the send. Copy the entry before doing that
                    if (tempBuffer.Length < entryLength) tempBuffer = new byte[entryLength];
                    new Span<byte>(entry, entryLength).CopyTo(tempBuffer);
                    it.UnsafeRelease();
                    
                    SendCurrentBuffer();
                    ResetSendBuffer();
                    
                    msgnum++;
                    *(long*) dcurr = lsn;
                    dcurr += sizeof(long);
                    *(long*) dcurr = nextLsn;
                    dcurr += sizeof(long);
                    *(DarqMessageType*) dcurr = type;
                    dcurr += sizeof(DarqMessageType);
                    *(int*) dcurr = entryLength;
                    dcurr += sizeof(int);
                    fixed (byte *b = tempBuffer)
                        Buffer.MemoryCopy(b, dcurr, entryLength, entryLength);
                    dcurr += entryLength;
                    Debug.Assert(dcurr < dend);
                }
                else
                {
                    msgnum++;
                    *(long*) dcurr = lsn;
                    dcurr += sizeof(long);
                    *(long*) dcurr = nextLsn;
                    dcurr += sizeof(long);
                    *(DarqMessageType*) dcurr = type;
                    dcurr += sizeof(DarqMessageType);
                    *(int*) dcurr = entryLength;
                    dcurr += sizeof(int);
                    Buffer.MemoryCopy(entry, dcurr, entryLength, entryLength);
                    it.UnsafeRelease();
                    dcurr += entryLength;
                    Debug.Assert(dcurr < dend);
                }

                return true;
            }
            return false;
        }
        

        public override unsafe void Publish(ref byte* keyPtr, int keyLength, ref byte* valPtr, int valLength,
            ref byte* inputPtr, int sid)
        {
            throw new System.NotImplementedException();
        }

        public override unsafe void PrefixPublish(byte* prefixPtr, int prefixLength, ref byte* keyPtr, int keyLength,
            ref byte* valPtr, int valLength,
            ref byte* inputPtr, int sid)
        {
            throw new System.NotImplementedException();
        }
    }
}