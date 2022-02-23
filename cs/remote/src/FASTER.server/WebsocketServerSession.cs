// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using FASTER.common;
using FASTER.core;
using System.Collections.Generic;

namespace FASTER.server
{
    internal struct Decoder
    {
        public int msgLen;
        public int maskStart;
        public int dataStart;
    };

    internal unsafe sealed class WebsocketServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>
        : FasterKVServerSessionBase<Key, Value, Input, Output, Functions, ParameterSerializer>
        where Functions : IFunctions<Key, Value, Input, Output, long>
        where ParameterSerializer : IServerSerializer<Key, Value, Input, Output>
    {
        readonly HeaderReaderWriter hrw;
        byte* recvBufferPtr;
        int readHead;

        int pendingSeqNo, msgnum, start;
        byte* dcurr;

        readonly SubscribeKVBroker<Key, Value, Input, IKeyInputSerializer<Key, Input>> subscribeKVBroker;
        readonly SubscribeBroker<Key, Value, IKeySerializer<Key>> subscribeBroker;

        public WebsocketServerSession(
            INetworkSender networkSender,
            FasterKV<Key, Value> store, 
            Functions functions, 
            ParameterSerializer serializer,            
            SubscribeKVBroker<Key, Value, Input, IKeyInputSerializer<Key, Input>> subscribeKVBroker, 
            SubscribeBroker<Key, Value, IKeySerializer<Key>> subscribeBroker)
            : base(networkSender, store, functions, null, serializer)
        {
            this.subscribeKVBroker = subscribeKVBroker;
            this.subscribeBroker = subscribeBroker;

            readHead = 0;

            // Reserve minimum 4 bytes to send pending sequence number as output
            if (this.networkSender.GetMaxSizeSettings.MaxOutputSize < sizeof(int))
                this.networkSender.GetMaxSizeSettings.MaxOutputSize = sizeof(int);
        }

        public override unsafe int TryConsumeMessages(byte* req_buf, int bytesReceived)
        {
            recvBufferPtr = req_buf;
            while (TryReadMessages(out var offset))
            {
                bool completeWSCommand = ProcessBatch(req_buf, bytesReceived, offset);
                if (!completeWSCommand)
                    return bytesRead;
            }

            // The bytes left in the current buffer not consumed by previous operations
            var bytesLeft = bytesRead - readHead;
            if (bytesLeft != bytesRead)
            {
                // Shift them to the head of the array so we can reset the buffer to a consistent state
                if (bytesLeft > 0)
                    Buffer.MemoryCopy(req_buf + readHead, req_buf, bytesLeft, bytesLeft);
                bytesRead = bytesLeft;
                readHead = 0;
            }

            return bytesRead;
        }

        public override void CompleteRead(ref Output output, long ctx, core.Status status)
        {            
            byte* d = networkSender.GetResponseObjectHead();
            var dend = networkSender.GetResponseObjectTail();

            if ((int)(dend - dcurr) < 7 + networkSender.GetMaxSizeSettings.MaxOutputSize)
                SendAndReset(ref d, ref dend);

            hrw.Write(MessageType.PendingResult, ref dcurr, (int)(dend - dcurr));
            hrw.Write((MessageType)(ctx >> 32), ref dcurr, (int)(dend - dcurr));
            Write((int)(ctx & 0xffffffff), ref dcurr, (int)(dend - dcurr));
            Write(ref status, ref dcurr, (int)(dend - dcurr));
            if (status.Found)
                serializer.Write(ref output, ref dcurr, (int)(dend - dcurr));
            msgnum++;
        }

        public override void CompleteRMW(ref Output output, long ctx, Status status)
        {
            byte* d = networkSender.GetResponseObjectHead();
            var dend = networkSender.GetResponseObjectTail();

            if ((int)(dend - dcurr) < 7 + networkSender.GetMaxSizeSettings.MaxOutputSize)
                SendAndReset(ref d, ref dend);

            hrw.Write(MessageType.PendingResult, ref dcurr, (int)(dend - dcurr));
            hrw.Write((MessageType)(ctx >> 32), ref dcurr, (int)(dend - dcurr));
            Write((int)(ctx & 0xffffffff), ref dcurr, (int)(dend - dcurr));
            Write(ref status, ref dcurr, (int)(dend - dcurr));
            if (status.IsCompletedSuccessfully)
                serializer.Write(ref output, ref dcurr, (int)(dend - dcurr));
            msgnum++;

            int packetLen = (int)((dcurr - 10) - d);
            CreateSendPacketHeader(ref d, packetLen);
        }

        private bool TryReadMessages(out int offset)
        {
            offset = default;

            var bytesAvailable = bytesRead - readHead;
            // Need to at least have read off of size field on the message
            if (bytesAvailable < sizeof(int)) return false;

            offset = readHead;
            return true;
        }

        private static unsafe void CreateSendPacketHeader(ref byte* d, int payloadLen)
        {
            if (payloadLen < 126)
            {
                d += 8;
            }
            else if (payloadLen < 65536)
            {
                d += 6;
            }
            byte* dcurr = d;

            *dcurr = 0b10000010;
            dcurr++;
            if (payloadLen < 126)
            {
                *dcurr = (byte)(payloadLen & 0b01111111);
                dcurr++;
            }
            else if (payloadLen < 65536)
            {
                *dcurr = (byte)(0b01111110);
                dcurr++;
                byte[] payloadLenBytes = BitConverter.GetBytes((UInt16)payloadLen);
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(payloadLenBytes);

                *dcurr++ = payloadLenBytes[0];
                *dcurr++ = payloadLenBytes[1];
            }
            else
            {
                *dcurr = (byte)(0b01111111);
                dcurr++;
                byte[] payloadLenBytes = BitConverter.GetBytes((UInt64)payloadLen);
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(payloadLenBytes);

                *dcurr++ = (byte)(payloadLenBytes[0] & 0b01111111);
                *dcurr++ = payloadLenBytes[1];
                *dcurr++ = payloadLenBytes[2];
                *dcurr++ = payloadLenBytes[3];
                *dcurr++ = payloadLenBytes[4];
                *dcurr++ = payloadLenBytes[5];
                *dcurr++ = payloadLenBytes[6];
                *dcurr++ = payloadLenBytes[7];
            }
        }

        private unsafe bool ProcessBatch(byte* buf, int length, int offset)
        {
            bool completeWSCommand = true;
            networkSender.GetResponseObject();
            
            byte* b = buf + offset;
            byte* d = networkSender.GetResponseObjectHead();
            var dend = networkSender.GetResponseObjectTail();
            dcurr = d; // reserve space for size
            var bytesAvailable = bytesRead - readHead;
            var _origReadHead = readHead;
            int msglen = 0;
            byte[] decoded = Array.Empty<byte>();
            var ptr = recvBufferPtr + readHead;
            var totalMsgLen = 0;
            List<Decoder> decoderInfoList = new();

            if (buf[offset] == 71 && buf[offset + 1] == 69 && buf[offset + 2] == 84)
            {
                // 1. Obtain the value of the "Sec-WebSocket-Key" request header without any leading or trailing whitespace
                // 2. Concatenate it with "258EAFA5-E914-47DA-95CA-C5AB0DC85B11" (a special GUID specified by RFC 6455)
                // 3. Compute SHA-1 and Base64 hash of the new value
                // 4. Write the hash back as the value of "Sec-WebSocket-Accept" response header in an HTTP response                

                //string s = Encoding.UTF8.GetString(buf, offset, length - offset);
                string s = Encoding.UTF8.GetString(new ReadOnlySpan<byte>((void*)(buf + offset), length - offset).ToArray());
                string swk = Regex.Match(s, "Sec-WebSocket-Key: (.*)").Groups[1].Value.Trim();
                string swka = swk + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
                byte[] swkaSha1 = System.Security.Cryptography.SHA1.Create().ComputeHash(Encoding.UTF8.GetBytes(swka));
                string swkaSha1Base64 = Convert.ToBase64String(swkaSha1);

                // HTTP/1.1 defines the sequence CR LF as the end-of-line marker
                byte[] response = Encoding.UTF8.GetBytes(
                    "HTTP/1.1 101 Switching Protocols\r\n" +
                    "Connection: Upgrade\r\n" +
                    "Upgrade: websocket\r\n" +
                    "Sec-WebSocket-Accept: " + swkaSha1Base64 + "\r\n\r\n");

                fixed (byte* responsePtr = &response[0])
                    Buffer.MemoryCopy(responsePtr, dcurr, response.Length, response.Length);

                dcurr += response.Length;

                networkSender.SendResponse((int)(d - networkSender.GetResponseObjectHead()), (int)(dcurr - d));
                readHead = bytesRead;
                return completeWSCommand;

            }
            else
            {
                var decoderInfo = new Decoder();

                bool fin = (buf[offset] & 0b10000000) != 0,
                    mask = (buf[offset + 1] & 0b10000000) != 0; // must be true, "All messages from the client to the server have this bit set"

                int opcode = buf[offset] & 0b00001111; // expecting 1 - text message
                offset++;

                msglen = buf[offset] - 128; // & 0111 1111

                if (msglen < 125)
                {
                    offset++;
                }
                else if (msglen == 126)
                {
                    msglen = BitConverter.ToUInt16(new byte[] { buf[offset + 2], buf[offset + 1] }, 0);
                    offset += 3;
                }
                else if (msglen == 127)
                {
                    msglen = (int)BitConverter.ToUInt64(new byte[] { buf[offset + 8], buf[offset + 7], buf[offset + 6], buf[offset + 5], buf[offset + 4], buf[offset + 3], buf[offset + 2], buf[offset + 1] }, 0);
                    offset += 9;
                }

                if (msglen == 0)
                    Console.WriteLine("msglen == 0");


                decoderInfo.maskStart = offset;
                decoderInfo.msgLen = msglen;
                decoderInfo.dataStart = offset + 4;
                decoderInfoList.Add(decoderInfo);
                totalMsgLen += msglen;
                offset += 4;

                if (fin == false)
                {
                    byte[] decodedClientMsgLen = new byte[sizeof(Int32)];
                    byte[] clientMsgLenMask = new byte[4] { buf[decoderInfo.maskStart], buf[decoderInfo.maskStart + 1], buf[decoderInfo.maskStart + 2], buf[decoderInfo.maskStart + 3] };
                    for (int i = 0; i < sizeof(Int32); ++i)
                        decodedClientMsgLen[i] = (byte)(buf[decoderInfo.dataStart + i] ^ clientMsgLenMask[i % 4]);
                    var clientMsgLen = (int)BitConverter.ToInt32(decodedClientMsgLen, 0);
                    if (clientMsgLen > bytesRead)
                        return false;
                }

                var nextBufOffset = offset;
                nextBufOffset += msglen;

                while (fin == false)
                {
                    fin = (buf[nextBufOffset] & 0b10000000) != 0;

                    nextBufOffset++;
                    int nextMsgLen = buf[nextBufOffset] & 0b01111111;

                    nextBufOffset++;

                    if (nextMsgLen < 125)
                    {
                        nextBufOffset++;
                    }
                    else if (nextMsgLen == 126)
                    {
                        nextMsgLen = BitConverter.ToUInt16(new byte[] { buf[nextBufOffset + 1], buf[nextBufOffset] }, 0);
                        nextBufOffset += 2;
                    }
                    else if (nextMsgLen == 127)
                    {
                        nextMsgLen = (int)BitConverter.ToUInt64(new byte[] { buf[nextBufOffset + 7], buf[nextBufOffset + 6], buf[nextBufOffset + 5], buf[nextBufOffset + 4], buf[nextBufOffset + 3], buf[nextBufOffset + 2], buf[nextBufOffset + 1], buf[nextBufOffset] }, 0);
                        nextBufOffset += 8;
                    }

                    var nextDecoderInfo = new Decoder
                    {
                        msgLen = nextMsgLen,
                        maskStart = nextBufOffset,
                        dataStart = nextBufOffset + 4
                    };
                    decoderInfoList.Add(nextDecoderInfo);
                    nextBufOffset += 4; // 4 bytes of masking
                    nextBufOffset += nextMsgLen; // remaining message length                }
                }

                if (msglen == 2)
                {
                    this.Dispose();
                    return false;
                }

                offset = nextBufOffset;
                completeWSCommand = true;

                var decodedIndex = 0;
                decoded = new byte[totalMsgLen];
                for (int decoderListIdx = 0; decoderListIdx < decoderInfoList.Count; decoderListIdx++)
                {
                    {
                        var decoderInfoElem = decoderInfoList[decoderListIdx];
                        byte[] masks = new byte[4] { buf[decoderInfoElem.maskStart], buf[decoderInfoElem.maskStart + 1], buf[decoderInfoElem.maskStart + 2], buf[decoderInfoElem.maskStart + 3] };

                        for (int i = 0; i < decoderInfoElem.msgLen; ++i)
                            decoded[decodedIndex++] = (byte)(buf[decoderInfoElem.dataStart + i] ^ masks[i % 4]);
                    }
                }

                readHead = offset;
            }

            dcurr = d;
            dcurr += 10;
            dcurr += sizeof(int); // reserve space for size
            int origPendingSeqNo = pendingSeqNo;

            dcurr += BatchHeader.Size;
            start = 0;
            msgnum = 0;

            fixed (byte* ptr1 = &decoded[4])
            {
                var src = ptr1;
                ref var header = ref Unsafe.AsRef<BatchHeader>(src);
                int num = *(int*)(src + 4);
                src += BatchHeader.Size;
                Status status = default;

                for (msgnum = 0; msgnum < num; msgnum++)
                {
                    var message = (MessageType)(*src++);

                    switch (message)
                    {
                        case MessageType.Upsert:
                        case MessageType.UpsertAsync:
                            if ((int)(dend - dcurr) < 2)
                                SendAndReset(ref d, ref dend);

                            var keyPtr = src;
                            status = session.Upsert(ref serializer.ReadKeyByRef(ref src), ref serializer.ReadValueByRef(ref src));

                            hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                            Write(ref status, ref dcurr, (int)(dend - dcurr));

                            if (subscribeKVBroker != null)
                                subscribeKVBroker.Publish(keyPtr);
                            break;

                        case MessageType.Read:
                        case MessageType.ReadAsync:
                            if ((int)(dend - dcurr) < 2 + networkSender.GetMaxSizeSettings.MaxOutputSize)
                                SendAndReset(ref d, ref dend);

                            long ctx = ((long)message << 32) | (long)pendingSeqNo;
                            status = session.Read(ref serializer.ReadKeyByRef(ref src), ref serializer.ReadInputByRef(ref src),
                                ref serializer.AsRefOutput(dcurr + 2, (int)(dend - dcurr)), ctx, 0);

                            hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                            Write(ref status, ref dcurr, (int)(dend - dcurr));

                            if (status.IsPending)
                                Write(pendingSeqNo++, ref dcurr, (int)(dend - dcurr));
                            else if (status.Found)
                                serializer.SkipOutput(ref dcurr);

                            break;

                        case MessageType.RMW:
                        case MessageType.RMWAsync:
                            if ((int)(dend - dcurr) < 2)
                                SendAndReset(ref d, ref dend);

                            keyPtr = src;

                            ctx = ((long)message << 32) | (long)pendingSeqNo;
                            status = session.RMW(ref serializer.ReadKeyByRef(ref src), ref serializer.ReadInputByRef(ref src), ctx);

                            hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                            Write(ref status, ref dcurr, (int)(dend - dcurr));
                            if (status.IsPending)
                                Write(pendingSeqNo++, ref dcurr, (int)(dend - dcurr));

                            if (subscribeKVBroker != null)
                                subscribeKVBroker.Publish(keyPtr);
                            break;

                        case MessageType.Delete:
                        case MessageType.DeleteAsync:
                            if ((int)(dend - dcurr) < 2)
                                SendAndReset(ref d, ref dend);

                            keyPtr = src;

                            status = session.Delete(ref serializer.ReadKeyByRef(ref src));

                            hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                            Write(ref status, ref dcurr, (int)(dend - dcurr));

                            if (subscribeKVBroker != null)
                                subscribeKVBroker.Publish(keyPtr);
                            break;

                        case MessageType.SubscribeKV:
                            Debug.Assert(subscribeKVBroker != null);

                            if ((int)(dend - dcurr) < 2 + networkSender.GetMaxSizeSettings.MaxOutputSize)
                                SendAndReset(ref d, ref dend);

                            var keyStart = src;
                            ref Key key = ref serializer.ReadKeyByRef(ref src);

                            var inputStart = src;
                            ref Input input = ref serializer.ReadInputByRef(ref src);

                            int sid = subscribeKVBroker.Subscribe(ref keyStart, ref inputStart, this);
                            status = Status.CreatePending();

                            hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                            Write(ref status, ref dcurr, (int)(dend - dcurr));
                            Write(sid, ref dcurr, (int)(dend - dcurr));
                            serializer.Write(ref key, ref dcurr, (int)(dend - dcurr));

                            break;

                        case MessageType.PSubscribeKV:
                            Debug.Assert(subscribeKVBroker != null);

                            if ((int)(dend - dcurr) < 2 + networkSender.GetMaxSizeSettings.MaxOutputSize)
                                SendAndReset(ref d, ref dend);

                            keyStart = src;
                            key = ref serializer.ReadKeyByRef(ref src);

                            inputStart = src;
                            input = ref serializer.ReadInputByRef(ref src);

                            sid = subscribeKVBroker.PSubscribe(ref keyStart, ref inputStart, this);
                            status = Status.CreatePending();

                            hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                            Write(ref status, ref dcurr, (int)(dend - dcurr));
                            Write(sid, ref dcurr, (int)(dend - dcurr));
                            serializer.Write(ref key, ref dcurr, (int)(dend - dcurr));

                            break;

                        case MessageType.Publish:
                            Debug.Assert(subscribeBroker != null);

                            if ((int)(dend - dcurr) < 2)
                                SendAndReset(ref d, ref dend);

                            keyPtr = src;
                            key = ref serializer.ReadKeyByRef(ref src);
                            byte* valPtr = src;
                            ref Value val = ref serializer.ReadValueByRef(ref src);
                            int valueLength = (int)(src - valPtr);

                            status = Status.CreateFound();
                            hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                            Write(ref status, ref dcurr, (int)(dend - dcurr));

                            if (subscribeBroker != null)
                                subscribeBroker.Publish(keyPtr, valPtr, valueLength);
                            break;

                        case MessageType.Subscribe:
                            Debug.Assert(subscribeBroker != null);

                            if ((int)(dend - dcurr) < 2 + networkSender.GetMaxSizeSettings.MaxOutputSize)
                                SendAndReset(ref d, ref dend);

                            keyStart = src;
                            serializer.ReadKeyByRef(ref src);

                            sid = subscribeBroker.Subscribe(ref keyStart, this);
                            status = Status.CreatePending();
                            hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                            Write(ref status, ref dcurr, (int)(dend - dcurr));
                            Write(sid, ref dcurr, (int)(dend - dcurr));
                            break;

                        case MessageType.PSubscribe:
                            Debug.Assert(subscribeBroker != null);

                            if ((int)(dend - dcurr) < 2 + networkSender.GetMaxSizeSettings.MaxOutputSize)
                                SendAndReset(ref d, ref dend);

                            keyStart = src;
                            serializer.ReadKeyByRef(ref src);

                            sid = subscribeBroker.PSubscribe(ref keyStart, this);
                            status = Status.CreatePending();
                            hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                            Write(ref status, ref dcurr, (int)(dend - dcurr));
                            Write(sid, ref dcurr, (int)(dend - dcurr));
                            break;

                        default:
                            throw new NotImplementedException();
                    }
                }
            }

            if (origPendingSeqNo != pendingSeqNo)
                session.CompletePending(true);

            // Send replies
            if (msgnum - start > 0)
                Send(d);
            else
            {
                networkSender.ReturnResponseObject();
            }

            return completeWSCommand;
        }

        /// <inheritdoc />
        public unsafe override void Publish(ref byte* keyPtr, int keyLength, ref byte* valPtr, int valLength, ref byte* inputPtr, int sid)
            => Publish(ref keyPtr, keyLength, ref valPtr, ref inputPtr, sid, false);

        /// <inheritdoc />
        public unsafe override void PrefixPublish(byte* prefixPtr, int prefixLength, ref byte* keyPtr, int keyLength, ref byte* valPtr, int valLength, ref byte* inputPtr, int sid)
            => Publish(ref keyPtr, keyLength, ref valPtr, ref inputPtr, sid, true);

        private unsafe void Publish(ref byte* keyPtr, int keyLength, ref byte* valPtr, ref byte* inputPtr, int sid, bool prefix)
        {
            MessageType message;

            if (valPtr == null)
            {
                message = MessageType.SubscribeKV;
                if (prefix)
                    message = MessageType.PSubscribeKV;
            }
            else
            {
                message = MessageType.Subscribe;
                if (prefix)
                    message = MessageType.PSubscribe;
            }

            networkSender.GetResponseObject();

            ref Key key = ref serializer.ReadKeyByRef(ref keyPtr);

            byte* d = networkSender.GetResponseObjectHead();
            var dend = networkSender.GetResponseObjectTail();
            var dcurr = d + 10 + sizeof(int); // reserve space for websocket header and size
            byte* outputDcurr;

            dcurr += BatchHeader.Size;

            long ctx = ((long)message << 32) | (long)sid;

            if (prefix)
                outputDcurr = dcurr + 6 + keyLength;
            else
                outputDcurr = dcurr + 6;

            Status status = Status.CreateFound();
            if (valPtr == null)
                status = session.Read(ref key, ref serializer.ReadInputByRef(ref inputPtr), ref serializer.AsRefOutput(outputDcurr, (int)(dend - dcurr)), ctx, 0);

            if (!status.IsPending)
            {
                // Write six bytes (message | status | sid)
                hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                Write(ref status, ref dcurr, (int)(dend - dcurr));
                Write(sid, ref dcurr, (int)(dend - dcurr));
                if (prefix)
                    serializer.Write(ref key, ref dcurr, (int)(dend - dcurr));
                if (valPtr != null)
                {
                    ref Value value = ref serializer.ReadValueByRef(ref valPtr);
                    serializer.Write(ref value, ref dcurr, (int)(dend - dcurr));
                }
                else if (status.Found)
                    serializer.SkipOutput(ref dcurr);
            }
            else
            {
                throw new Exception("Pending reads not supported with pub/sub");
            }

            // Send replies
            var dtemp = d + 10;
            var dstart = dtemp + sizeof(int);
            Unsafe.AsRef<BatchHeader>(dstart).NumMessages = 1;
            Unsafe.AsRef<BatchHeader>(dstart).SeqNo = 0;
            int packetLen = (int)((dcurr - 10) - d);

            CreateSendPacketHeader(ref d, packetLen);

            *(int*)dtemp = (packetLen - sizeof(int));
            networkSender.SendResponse((int) (d - networkSender.GetResponseObjectHead()), (int)(dcurr - d));
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe bool WriteOpSeqId(ref int o, ref byte* dst, int length)
        {
            if (length < sizeof(int)) return false;
            *(int*)dst = o;
            dst += sizeof(int);
            return true;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe bool Write(ref Status s, ref byte* dst, int length)
        {
            if (length < 1) return false;
            *dst++ = s.Value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe bool Write(int seqNo, ref byte* dst, int length)
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
            networkSender.GetResponseObject();
            d = networkSender.GetResponseObjectHead();
            dend = networkSender.GetResponseObjectTail();
            dcurr = d;
            dcurr += 10;
            dcurr += sizeof(int); // reserve space for size
            start = msgnum;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Send(byte* d)
        {
            if ((int)(dcurr - d) > 0)
            {
                int packetLen = (int)((dcurr - 10) - d);
                var dtemp = d + 10;
                var dstart = dtemp + sizeof(int);

                CreateSendPacketHeader(ref d, packetLen);

                *(int*)dtemp = (packetLen - sizeof(int));
                *(int*)dstart = 0;
                *(int*)(dstart + sizeof(int)) = (msgnum - start);
                networkSender.SendResponse((int)(d - networkSender.GetResponseObjectHead()), (int)(dcurr - d));
            }
        }
    }
}
