using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using FASTER.common;
using FASTER.libdpr;

namespace FASTER.server
{
    internal sealed unsafe class DarqServerSession : ServerSessionBase
    {
        readonly HeaderReaderWriter hrw;
        int readHead;
        int seqNo, msgnum, start;
        private DprServer<Darq> dprServer;

        public DarqServerSession(INetworkSender networkSender, DprServer<Darq> dprServer) : base(networkSender)
        {
            this.dprServer = dprServer;
        }

        public override int TryConsumeMessages(byte* req_buf, int bytesReceived)
        {
            bytesRead = bytesReceived;
            readHead = 0;
            while (TryReadMessages(req_buf, out var offset))
                ProcessBatch(req_buf, offset);
            return readHead;
        }

        private bool TryReadMessages(byte* buf, out int offset)
        {
            offset = default;

            var bytesAvailable = bytesRead - readHead;
            // Need to at least have read off of size field on the message
            if (bytesAvailable < sizeof(int)) return false;

            // MSB is 1 to indicate binary protocol
            var size = -(*(int*) (buf + readHead));

            // Not all of the message has arrived
            if (bytesAvailable < size + sizeof(int)) return false;
            offset = readHead + sizeof(int);

            // Consume this message and the header
            readHead += size + sizeof(int);
            return true;
        }

        private int numStepped = 0;
        private void ProcessBatch(byte* buf, int offset)
        {
            networkSender.GetResponseObject();

            byte* b = buf + offset;
            byte* d = networkSender.GetResponseObjectHead();
            var dend = networkSender.GetResponseObjectTail();
            var dcurr = d + sizeof(int); // reserve space for size

            var src = b;
            ref var header = ref Unsafe.AsRef<BatchHeader>(src);
            var num = header.NumMessages;
            src += BatchHeader.Size;
            dcurr += BatchHeader.Size;
            
            var dprResponseOffset = (int *) dcurr;
            dcurr += sizeof(int);
            start = 0;
            msgnum = 0;

            var dprHeaderSize = *(int*) src;
            src += sizeof(int);
            var request = new ReadOnlySpan<byte>(src, dprHeaderSize);
            // Error code path
            if (!dprServer.RequestRemoteBatchBegin(request, out var tracker))
            {
                // No response body
                *dprResponseOffset = 0;
                dcurr += sizeof(int);
                var responseSize = dprServer.ComposeErrorResponse(request, new Span<byte>(dcurr, (int) (dend - dcurr)));
                Debug.Assert(responseSize > 0);
                dcurr += responseSize;
                *(dprResponseOffset + 1) = responseSize;
                Send(d, dcurr);
                return;
            }
            src += dprHeaderSize;
            
            // TODO(Tianyu): Handle consumer id  mismatch cases
            for (msgnum = 0; msgnum < num; msgnum++)
            {
                var message = (MessageType) (*src++);
                switch (message)
                {
                    case MessageType.DarqStep:
                    {
                        var processorId = *(long*) src;
                        src += sizeof(long);
                        
                        var batch = new SerializedDarqEntryBatch(src);
                        var response = dprServer.StateObject().Step(processorId, batch);
                        tracker.MarkOneOperationVersion(msgnum, response.version);
                        ++numStepped;
                        if (numStepped % 100000 == 0)
                            Console.WriteLine($"DARQ server processed {numStepped} steps");
                        
                        hrw.Write(message, ref dcurr, (int) (dend - dcurr));
                        *(StepStatus *) dcurr = response.status;
                        dcurr += sizeof(StepStatus);
                        break;
                    }
                    case MessageType.DarqEnqueue:
                    {
                        var worker = new Worker(*(long*) src);
                        src += sizeof(long);

                        var lsn = *(long*) src;
                        src += sizeof(long);

                        var batch = new SerializedDarqEntryBatch(src);
                        long version;

                        dprServer.StateObject()
                                .EnqueueInputBatch(batch, worker, lsn, out version);
                        tracker.MarkOneOperationVersion(msgnum, version);
                        src += batch.TotalSize();
                        hrw.Write(message, ref dcurr, (int) (dend - dcurr));
                        break;
                    }
                    case MessageType.DarqRegisterProcessor:
                    {
                        var consumerId = dprServer.StateObject().RegisterNewProcessor();
                        hrw.Write(message, ref dcurr, (int) (dend - dcurr));
                        *(long*) dcurr = consumerId;
                        dcurr += sizeof(long);
                        break;
                    }
                    default:
                        throw new NotImplementedException();
                }
            }

            // write down start of DPR header
            *dprResponseOffset = (int) (dcurr - (byte*)dprResponseOffset) - sizeof(int);
            
            // reserve a size field for DPR header;
            var dprHeaderSizeField = (int*)dcurr;
            dcurr += sizeof(int);
            var respSize = dprServer.SignalRemoteBatchFinish(request, new Span<byte>(dcurr, (int) (dend - dcurr)), tracker);
            Debug.Assert(respSize > 0);
            dcurr += respSize;
            // Write size
            *dprHeaderSizeField = respSize;
            // Send replies
            Send(d, dcurr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Send(byte* d, byte* dcurr)
        {
            var dstart = d + sizeof(int);
            ((BatchHeader*) dstart)->SetNumMessagesProtocol(msgnum - start, WireFormat.DARQWrite);
            ((BatchHeader*) dstart)->SeqNo = seqNo++;
            int payloadSize = (int) (dcurr - d);
            // Set packet size in header
            *(int*) networkSender.GetResponseObjectHead() = -(payloadSize - sizeof(int));
            if (!networkSender.SendResponse(0, payloadSize))
            {
                throw new ObjectDisposedException("socket closed");
            }
        }


        public override void Publish(ref byte* keyPtr, int keyLength, ref byte* valPtr, int valLength,
            ref byte* inputPtr, int sid)
        {
            throw new System.NotImplementedException();
        }

        public override void PrefixPublish(byte* prefixPtr, int prefixLength, ref byte* keyPtr, int keyLength,
            ref byte* valPtr, int valLength,
            ref byte* inputPtr, int sid)
        {
            throw new System.NotImplementedException();
        }
    }
}