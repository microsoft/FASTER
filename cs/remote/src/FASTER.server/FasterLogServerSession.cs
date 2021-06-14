using System;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using FASTER.common;
using FASTER.core;

namespace FASTER.server
{
    internal unsafe class FasterLogServerSession : ServerSessionBase
    {
        private FasterLog log;
         readonly HeaderReaderWriter hrw;
        int readHead;

        int seqNo, pendingSeqNo, msgnum, start;
        byte* dcurr;

        public FasterLogServerSession(Socket socket, FasterLog log, MaxSizeSettings maxSizeSettings)
            : base(socket, maxSizeSettings)
        {
            this.log = log;
            readHead = 0;
            // Reserve minimum 4 bytes to send pending sequence number as output
            if (this.maxSizeSettings.MaxOutputSize < sizeof(int))
                this.maxSizeSettings.MaxOutputSize = sizeof(int);
        }

        public override int TryConsumeMessages(byte[] buf)
        {
            while (TryReadMessages(buf, out var offset))
                ProcessBatch(buf, offset);

            // The bytes left in the current buffer not consumed by previous operations
            var bytesLeft = bytesRead - readHead;
            if (bytesLeft != bytesRead)
            {
                // Shift them to the head of the array so we can reset the buffer to a consistent state
                Array.Copy(buf, readHead, buf, 0, bytesLeft);
                bytesRead = bytesLeft;
                readHead = 0;
            }

            return bytesRead;
        }
        
        private bool TryReadMessages(byte[] buf, out int offset)
        {
            offset = default;

            var bytesAvailable = bytesRead - readHead;
            // Need to at least have read off of size field on the message
            if (bytesAvailable < sizeof(int)) return false;

            // MSB is 1 to indicate binary protocol
            var size = -BitConverter.ToInt32(buf, readHead);
            // Not all of the message has arrived
            if (bytesAvailable < size + sizeof(int)) return false;
            offset = readHead + sizeof(int);

            // Consume this message and the header
            readHead += size + sizeof(int);
            return true;
        }


        private void ProcessBatch(byte[] buf, int offset)
        {
GetResponseObject();

            fixed (byte* b = &buf[offset])
            {
                byte* d = responseObject.obj.bufferPtr;
                var dend = d + responseObject.obj.buffer.Length;
                dcurr = d + sizeof(int); // reserve space for size
                int origPendingSeqNo = pendingSeqNo;

                var src = b;
                ref var header = ref Unsafe.AsRef<BatchHeader>(src);
                src += BatchHeader.Size;
                core.Status status = default;

                dcurr += BatchHeader.Size;
                start = 0;
                msgnum = 0;
                for (msgnum = 0; msgnum < header.GetNumMessages(); msgnum++)
                {
                    var message = (MessageType)(*src++);
                    switch (message)
                    {
                        case MessageType.LogEnqueue:
                        case MessageType.LogEnqueueAsync:
                            if ((int)(dend - dcurr) < 2)
                                SendAndReset(ref d, ref dend);
                            session.
                            status = session.Upsert(ref serializer.ReadKeyByRef(ref src), ref serializer.ReadValueByRef(ref src));
                            hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                            Write(ref status, ref dcurr, (int)(dend - dcurr));
                            break;

                        case MessageType.Read:
                        case MessageType.ReadAsync:
                            if ((int)(dend - dcurr) < 2 + maxSizeSettings.MaxOutputSize)
                                SendAndReset(ref d, ref dend);

                            long ctx = ((long)message << 32) | (long)pendingSeqNo;
                            status = session.Read(ref serializer.ReadKeyByRef(ref src), ref serializer.ReadInputByRef(ref src),
                                ref serializer.AsRefOutput(dcurr + 2, (int)(dend - dcurr)), ctx, 0);

                            hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                            Write(ref status, ref dcurr, (int)(dend - dcurr));

                            if (status == core.Status.PENDING)
                                Write(pendingSeqNo++, ref dcurr, (int)(dend - dcurr));
                            else if (status == core.Status.OK)
                                serializer.SkipOutput(ref dcurr);
                            break;

                        case MessageType.RMW:
                        case MessageType.RMWAsync:
                            if ((int)(dend - dcurr) < 2)
                                SendAndReset(ref d, ref dend);

                            ctx = ((long)message << 32) | (long)pendingSeqNo;
                            status = session.RMW(ref serializer.ReadKeyByRef(ref src), ref serializer.ReadInputByRef(ref src), ctx);

                            hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                            Write(ref status, ref dcurr, (int)(dend - dcurr));
                            if (status == core.Status.PENDING)
                                Write(pendingSeqNo++, ref dcurr, (int)(dend - dcurr));
                            break;

                        case MessageType.Delete:
                        case MessageType.DeleteAsync:
                            if ((int)(dend - dcurr) < 2)
                                SendAndReset(ref d, ref dend);

                            status = session.Delete(ref serializer.ReadKeyByRef(ref src));
                            hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                            Write(ref status, ref dcurr, (int)(dend - dcurr));
                            break;

                        default:
                            throw new NotImplementedException();
                    }
                }

                if (origPendingSeqNo != pendingSeqNo)
                    session.CompletePending(true);

                // Send replies
                if (msgnum - start > 0)
                    Send(d);
                else
                    responseObject.Dispose();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe bool Write(ref core.Status s, ref byte* dst, int length)
        {
            if (length < 1) return false;
            *dst++ = (byte)s;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe bool Write(int seqNo, ref byte* dst, int length)
        {
            if (length < sizeof(int)) return false;
            *(int*)dst = seqNo;
            dst += sizeof(int);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SendAndReset(ref byte* d, ref byte* dend)
        {
            Send(d);
            GetResponseObject();
            d = responseObject.obj.bufferPtr;
            dend = d + responseObject.obj.buffer.Length;
            dcurr = d + sizeof(int);
            start = msgnum;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Send(byte* d)
        {
            var dstart = d + sizeof(int);
            Unsafe.AsRef<BatchHeader>(dstart) = new BatchHeader(seqNo++, msgnum - start, WireFormat.Binary);
            int payloadSize = (int)(dcurr - d);
            // Set packet size in header
            *(int*)responseObject.obj.bufferPtr = -(payloadSize - sizeof(int));
            SendResponse(payloadSize);
            responseObject.obj = null;
        }

    }
}