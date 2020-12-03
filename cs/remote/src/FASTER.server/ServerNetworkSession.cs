// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using FASTER.common;
using FASTER.core;

namespace FASTER.server
{
    internal unsafe class ServerNetworkSession<Key, Value, Input, Output, Functions, ParameterSerializer>
        where Functions : IFunctions<Key, Value, Input, Output, long>
        where ParameterSerializer : IServerSerializer<Key, Value, Input, Output>
    {
        private readonly Socket socket;
        private readonly ClientSession<Key, Value, Input, Output, long, ServerFunctions<Key, Value, Input, Output, Functions, ParameterSerializer>> session;
        private readonly NetworkSender messageManager;
        private readonly ParameterSerializer serializer;
        private readonly MaxSizeSettings maxSizeSettings;
        private readonly int serverBufferSize;
        readonly HeaderReaderWriter hrw;

        private int bytesRead;
        private int readHead;

        public ServerNetworkSession(Socket socket, FasterKV<Key, Value> store, Functions functions, ParameterSerializer serializer, MaxSizeSettings maxSizeSettings)
        {
            this.socket = socket;
            session = store.For(new ServerFunctions<Key, Value, Input, Output, Functions, ParameterSerializer>(functions, this)).NewSession<ServerFunctions<Key, Value, Input, Output, Functions, ParameterSerializer>>();
            this.maxSizeSettings = maxSizeSettings;
            // Reserve minimum 4 bytes to send pending sequence number as output
            if (this.maxSizeSettings.MaxOutputSize < sizeof(int))
                this.maxSizeSettings.MaxOutputSize = sizeof(int);
            this.serverBufferSize = Utils.ServerBufferSize(maxSizeSettings);
            this.messageManager = new NetworkSender(serverBufferSize);
            this.serializer = serializer;

            bytesRead = 0;
            readHead = 0;
        }

        internal void AddBytesRead(int bytesRead) => this.bytesRead += bytesRead;

        internal int TryConsumeMessages(byte[] buf)
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

            var size = BitConverter.ToInt32(buf, readHead);
            // Not all of the message has arrived
            if (bytesAvailable < size + sizeof(int)) return false;
            offset = readHead + sizeof(int);

            // Consume this message and the header
            readHead += size + sizeof(int);
            return true;
        }

        int payloadSize;
        int seqNo;
        int pendingSeqNo;

        private unsafe void ProcessBatch(byte[] buf, int offset)
        {
            sendObject = messageManager.GetReusableSeaaBuffer();

            fixed (byte* b = &buf[offset])
            {
                byte* d = sendObject.obj.bufferPtr;
                var dend = d + sendObject.obj.buffer.Length;
                byte* dstart = d + sizeof(int); // reserve space for size
                byte* dcurr = dstart;
                int origPendingSeqNo = pendingSeqNo;

                var src = b;
                ref var header = ref Unsafe.AsRef<BatchHeader>(src);
                src += BatchHeader.Size;
                core.Status status = default;

                dcurr += BatchHeader.Size;
                int start = 0, msgnum = 0;
                for (msgnum = 0; msgnum < header.numMessages; msgnum++)
                {
                    var message = (MessageType)(*src++);
                    switch (message)
                    {
                        case MessageType.Upsert:
                        case MessageType.UpsertAsync:
                            if ((int)(dend - dcurr) < 2)
                            {
                                Unsafe.AsRef<BatchHeader>(dstart).numMessages = msgnum - start;
                                Unsafe.AsRef<BatchHeader>(dstart).seqNo = seqNo++;
                                messageManager.Send(socket, sendObject, (int)(dcurr - d));
                                sendObject = messageManager.GetReusableSeaaBuffer();
                                d = sendObject.obj.bufferPtr;
                                dend = d + sendObject.obj.buffer.Length;
                                dstart = dcurr = d + sizeof(int);
                                start = msgnum;
                            }

                            status = session.Upsert(ref serializer.ReadKeyByRef(ref src), ref serializer.ReadValueByRef(ref src));
                            hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                            Write(ref status, ref dcurr, (int)(dend - dcurr));
                            break;

                        case MessageType.Read:
                        case MessageType.ReadAsync:
                            if ((int)(dend - dcurr) < 2 + maxSizeSettings.MaxOutputSize)
                            {
                                Unsafe.AsRef<BatchHeader>(dstart).numMessages = msgnum - start;
                                Unsafe.AsRef<BatchHeader>(dstart).seqNo = seqNo++;
                                messageManager.Send(socket, sendObject, (int)(dcurr - d));
                                sendObject = messageManager.GetReusableSeaaBuffer();
                                d = sendObject.obj.bufferPtr;
                                dend = d + sendObject.obj.buffer.Length;
                                dstart = dcurr = d + sizeof(int);
                                start = msgnum;
                            }

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
                            {
                                Unsafe.AsRef<BatchHeader>(dstart).numMessages = msgnum - start;
                                Unsafe.AsRef<BatchHeader>(dstart).seqNo = seqNo++;
                                messageManager.Send(socket, sendObject, (int)(dcurr - d));
                                sendObject = messageManager.GetReusableSeaaBuffer();
                                d = sendObject.obj.bufferPtr;
                                dend = d + sendObject.obj.buffer.Length;
                                dstart = dcurr = d + sizeof(int);
                                start = msgnum;
                            }

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
                            {
                                Unsafe.AsRef<BatchHeader>(dstart).numMessages = msgnum - start;
                                Unsafe.AsRef<BatchHeader>(dstart).seqNo = seqNo++;
                                messageManager.Send(socket, sendObject, (int)(dcurr - d));
                                sendObject = messageManager.GetReusableSeaaBuffer();
                                d = sendObject.obj.bufferPtr;
                                dend = d + sendObject.obj.buffer.Length;
                                dstart = dcurr = d + sizeof(int);
                                start = msgnum;
                            }

                            status = session.Delete(ref serializer.ReadKeyByRef(ref src));
                            hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                            Write(ref status, ref dcurr, (int)(dend - dcurr));
                            break;

                        default:
                            throw new NotImplementedException();
                    }
                }

                if (origPendingSeqNo != pendingSeqNo)
                {
                    this.start = start;
                    this.msgnum = msgnum;
                    this.dcurr = dcurr;
                    session.CompletePending(true);
                    start = this.start;
                    msgnum = this.msgnum;
                    dcurr = this.dcurr;
                }

                // Send replies
                if (msgnum - start > 0)
                {
                    Unsafe.AsRef<BatchHeader>(dstart).numMessages = msgnum - start;
                    Unsafe.AsRef<BatchHeader>(dstart).seqNo = seqNo++;
                    payloadSize = (int)(dcurr - d);
                    messageManager.Send(socket, sendObject, payloadSize);
                }
            }
        }

        ReusableObject<SeaaBuffer> sendObject;
        int msgnum, start;
        byte* dcurr;

        internal void CompleteRead(ref Output output, long ctx, core.Status status)
        {
            byte* d = sendObject.obj.bufferPtr;
            var dend = d + sendObject.obj.buffer.Length;
            
            if ((int)(dend - dcurr) < 7 + maxSizeSettings.MaxOutputSize)
            {
                byte* dstart = d + sizeof(int); // reserve space for size
                Unsafe.AsRef<BatchHeader>(dstart).numMessages = msgnum - start;
                Unsafe.AsRef<BatchHeader>(dstart).seqNo = seqNo++;
                messageManager.Send(socket, sendObject, (int)(dcurr - d));
                sendObject = messageManager.GetReusableSeaaBuffer();
                d = sendObject.obj.bufferPtr;
                dend = d + sendObject.obj.buffer.Length;
                start = msgnum;
            }

            hrw.Write(MessageType.PendingResult, ref dcurr, (int)(dend - dcurr));
            hrw.Write((MessageType)(ctx>>32), ref dcurr, (int)(dend - dcurr));
            Write((int)(ctx & 0xffffffff), ref dcurr, (int)(dend - dcurr));
            Write(ref status, ref dcurr, (int)(dend - dcurr));
            if (status != core.Status.NOTFOUND)
                serializer.Write(ref output, ref dcurr, (int)(dend - dcurr));
            msgnum++;
        }

        internal void CompleteRMW(long ctx, core.Status status)
        {
            byte* d = sendObject.obj.bufferPtr;
            var dend = d + sendObject.obj.buffer.Length;

            if ((int)(dend - dcurr) < 7)
            {
                byte* dstart = d + sizeof(int); // reserve space for size
                Unsafe.AsRef<BatchHeader>(dstart).numMessages = msgnum - start;
                Unsafe.AsRef<BatchHeader>(dstart).seqNo = seqNo++;
                messageManager.Send(socket, sendObject, (int)(dcurr - d));
                sendObject = messageManager.GetReusableSeaaBuffer();
                d = sendObject.obj.bufferPtr;
                dend = d + sendObject.obj.buffer.Length;
                start = msgnum;
            }

            hrw.Write(MessageType.PendingResult, ref dcurr, (int)(dend - dcurr));
            hrw.Write((MessageType)(ctx >> 32), ref dcurr, (int)(dend - dcurr));
            Write((int)(ctx & 0xffffffff), ref dcurr, (int)(dend - dcurr));
            Write(ref status, ref dcurr, (int)(dend - dcurr));
            msgnum++;
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
        private static bool HandleReceiveCompletion(SocketAsyncEventArgs e)
        {
            var connState = (ServerNetworkSession<Key, Value, Input, Output, Functions, ParameterSerializer>)e.UserToken;
            if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success)
            {
                connState.socket.Dispose();
                e.Dispose();
                return false;
            }

            connState.AddBytesRead(e.BytesTransferred);
            var newHead = connState.TryConsumeMessages(e.Buffer);
            e.SetBuffer(newHead, e.Buffer.Length - newHead);
            return true;
        }

        public static void RecvEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            var connState = (ServerNetworkSession<Key, Value, Input, Output, Functions, ParameterSerializer>)e.UserToken;
            do
            {
                // No more things to receive
                if (!HandleReceiveCompletion(e)) break;
            } while (!connState.socket.ReceiveAsync(e));
        }
    }

}
