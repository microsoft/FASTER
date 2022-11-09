using System.Runtime.CompilerServices;
using FASTER.common;
using FASTER.darq;
using FASTER.libdpr;

namespace FASTER.server
{
    internal sealed unsafe class DarqProcessorSession : ServerSessionBase
    {
        readonly HeaderReaderWriter hrw;
        int readHead;
        int seqNo, msgnum, start;
        private Darq darq;

        public DarqProcessorSession(INetworkSender networkSender, Darq darq) : base(networkSender)
        {
            this.darq = darq;
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
            var size = -(*(int*)(buf + readHead));

            // Not all of the message has arrived
            if (bytesAvailable < size + sizeof(int)) return false;
            offset = readHead + sizeof(int);

            // Consume this message and the header
            readHead += size + sizeof(int);
            return true;
        }

        private void ProcessBatch(byte* buf, int offset)
        {
            var d = networkSender.GetResponseObjectHead();
            byte* b = buf + offset;
            var dend = networkSender.GetResponseObjectTail();
            var dcurr = d + sizeof(int); // reserve space for size

            var src = b;
            ref var header = ref Unsafe.AsRef<BatchHeader>(src);
            var num = header.NumMessages;
            src += BatchHeader.Size;
            dcurr += BatchHeader.Size;

            var dprResponseOffset = (int*)dcurr;
            dcurr += DprBatchHeader.FixedLenSize;
            start = 0;
            msgnum = 0;

            var dprHeaderSize = *(int*)src;
            src += sizeof(int);
            var request = new ReadOnlySpan<byte>(src, dprHeaderSize);
            // Error code path
            if (!darq.ReceiveAndBeginProcessing(request))
            {
                darq.ComposeErrorResponse(request, new Span<byte>(dprResponseOffset, DprBatchHeader.FixedLenSize));
                Send(d, dcurr);
                return;
            }

            src += dprHeaderSize;

            for (msgnum = 0; msgnum < num; msgnum++)
            {
                var message = (MessageType)(*src++);
                switch (message)
                {
                    case MessageType.DarqStep:
                    {
                        var processorId = *(long*)src;
                        src += sizeof(long);

                        var batch = new SerializedDarqEntryBatch(src);
                        var response = darq.Step(processorId, batch);
                        hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                        *(StepStatus*)dcurr = response;
                        dcurr += sizeof(StepStatus);
                        break;
                    }
                    case MessageType.DarqRegisterProcessor:
                    {
                        var consumerId = darq.RegisterNewProcessor();
                        hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                        *(long*)dcurr = consumerId;
                        dcurr += sizeof(long);
                        break;
                    }
                    default:
                        throw new NotImplementedException();
                }
            }


            darq.FinishProcessingAndSend(new Span<byte>(dprResponseOffset, DprBatchHeader.FixedLenSize));
            // Send replies
            Send(d, dcurr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Send(byte* d, byte* dcurr)
        {
            var dstart = d + sizeof(int);
            ((BatchHeader*)dstart)->SetNumMessagesProtocol(msgnum - start, WireFormat.DarqProcessor);
            ((BatchHeader*)dstart)->SeqNo = seqNo++;
            int payloadSize = (int)(dcurr - d);
            // Set packet size in header
            *(int*)networkSender.GetResponseObjectHead() = -(payloadSize - sizeof(int));
            if (!networkSender.SendResponse(0, payloadSize))
                throw new ObjectDisposedException("socket closed");
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