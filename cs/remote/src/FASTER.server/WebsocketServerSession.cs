// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using FASTER.common;
using FASTER.core;
using System.Collections.Concurrent;
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
        GCHandle recvHandle;
        byte* recvBufferPtr;
        int readHead;

        int seqNo, pendingSeqNo, msgnum, start;
        byte* dcurr;

        readonly SubscribeKVBroker<Key, Value, IKeySerializer<Key>> subscribeKVBroker;

        public WebsocketServerSession(Socket socket, FasterKV<Key, Value> store, Functions functions, ParameterSerializer serializer, MaxSizeSettings maxSizeSettings, SubscribeKVBroker<Key, Value, IKeySerializer<Key>> subscribeKVBroker)
            : base(socket, store, functions, serializer, maxSizeSettings)
        {
            this.subscribeKVBroker = subscribeKVBroker;

            readHead = 0;

            // Reserve minimum 4 bytes to send pending sequence number as output
            if (this.maxSizeSettings.MaxOutputSize < sizeof(int))
                this.maxSizeSettings.MaxOutputSize = sizeof(int);
        }

        public override int TryConsumeMessages(byte[] buf)
        {
            if (recvBufferPtr == null)
            {
                recvHandle = GCHandle.Alloc(buf, GCHandleType.Pinned);
                recvBufferPtr = (byte*)recvHandle.AddrOfPinnedObject();
            }
            ProcessBatch(buf, 0);

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

        public override void CompleteRead(ref Output output, long ctx, core.Status status)
        {
            byte* d = responseObject.obj.bufferPtr;
            var dend = d + responseObject.obj.buffer.Length;

            if ((int)(dend - dcurr) < 7 + maxSizeSettings.MaxOutputSize)
                SendAndReset(ref d, ref dend);

            hrw.Write(MessageType.PendingResult, ref dcurr, (int)(dend - dcurr));
            hrw.Write((MessageType)(ctx >> 32), ref dcurr, (int)(dend - dcurr));
            Write((int)(ctx & 0xffffffff), ref dcurr, (int)(dend - dcurr));
            Write(ref status, ref dcurr, (int)(dend - dcurr));
            if (status != core.Status.NOTFOUND)
                serializer.Write(ref output, ref dcurr, (int)(dend - dcurr));
            msgnum++;
        }

        public override void CompleteRMW(ref Output output, long ctx, Status status)
        {
            byte* d = responseObject.obj.bufferPtr;
            var dend = d + responseObject.obj.buffer.Length;

            if ((int)(dend - dcurr) < 7 + maxSizeSettings.MaxOutputSize)
                SendAndReset(ref d, ref dend);

            hrw.Write(MessageType.PendingResult, ref dcurr, (int)(dend - dcurr));
            hrw.Write((MessageType)(ctx >> 32), ref dcurr, (int)(dend - dcurr));
            Write((int)(ctx & 0xffffffff), ref dcurr, (int)(dend - dcurr));
            Write(ref status, ref dcurr, (int)(dend - dcurr));
            if (status == Status.OK || status == Status.NOTFOUND)
                serializer.Write(ref output, ref dcurr, (int)(dend - dcurr));
            msgnum++;

            int packetLen = (int)((dcurr - 10) - d);
            CreateSendPacketHeader(ref d, packetLen);
        }


        private bool TryReadMessages(byte[] buf, out int offset)
        {
            offset = default;

            var bytesAvailable = bytesRead - readHead;
            // Need to at least have read off of size field on the message
            if (bytesAvailable < sizeof(int)) return false;

            // MSB is 1 to indicate binary protocol
            var size = BitConverter.ToInt32(buf, readHead);
            // Not all of the message has arrived
            if (bytesAvailable < size + sizeof(int)) return false;
            offset = readHead + sizeof(int);

            // Consume this message and the header
            readHead += size + sizeof(int);
            return true;
        }

        private unsafe void CreateSendPacketHeader(ref byte* d, int payloadLen)
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

        private unsafe void ProcessBatch(byte[] buf, int offset)
        {
            GetResponseObject();

            fixed (byte* b = &buf[offset])
            {
                byte* d = responseObject.obj.bufferPtr;
                var dend = d + responseObject.obj.buffer.Length;
                dcurr = d; // reserve space for size
                var bytesAvailable = bytesRead - readHead;
                var _origReadHead = readHead;
                int origPendingSeqNo = pendingSeqNo;
                int msglen = 0;
                int bufOffset = 0;
                byte[] decoded = new byte[0];
                var ptr = recvBufferPtr + readHead;
                var totalMsgLen = 0;
                List<Decoder> decoderInfoList = new List<Decoder>();

                if (buf[0] == 71 && buf[1] == 69 && buf[2] == 84)
                {
                    // 1. Obtain the value of the "Sec-WebSocket-Key" request header without any leading or trailing whitespace
                    // 2. Concatenate it with "258EAFA5-E914-47DA-95CA-C5AB0DC85B11" (a special GUID specified by RFC 6455)
                    // 3. Compute SHA-1 and Base64 hash of the new value
                    // 4. Write the hash back as the value of "Sec-WebSocket-Accept" response header in an HTTP response
                    string s = Encoding.UTF8.GetString(buf);
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
                    //Debug.WriteLine("RESP: [" + Encoding.UTF8.GetString(new Span<byte>(d, (int)(dcurr - d)).ToArray()) + "]\n\n");
                    //Debug.WriteLine("RESP: [" + Encoding.UTF8.GetString(response) + "]\n\n");

                    ptr += bytesRead;
                    readHead = (int)(ptr - recvBufferPtr);
                    _origReadHead = readHead;
                    goto sendReply;
                }
                else
                {
                    var decoderInfo = new Decoder();

                    bool fin = (buf[0] & 0b10000000) != 0,
                        mask = (buf[1] & 0b10000000) != 0; // must be true, "All messages from the client to the server have this bit set"

                    int opcode = buf[0] & 0b00001111; // expecting 1 - text message
                    ptr++;

                    msglen = buf[1] - 128; // & 0111 1111

                    bufOffset = 2;

                    if (msglen < 125)
                    {
                        ptr++;
                    }
                    else if (msglen == 126)
                    {
                        ptr += 3;
                        msglen = BitConverter.ToUInt16(new byte[] { buf[3], buf[2] }, 0);
                        bufOffset = 4;
                    }
                    else if (msglen == 127)
                    {
                        ptr += 9;
                        msglen = (int)BitConverter.ToUInt64(new byte[] { buf[9], buf[8], buf[7], buf[6], buf[5], buf[4], buf[3], buf[2] }, 0);
                        bufOffset = 10;
                    }

                    if (msglen == 0)
                        Console.WriteLine("msglen == 0");


                    decoderInfo.maskStart = bufOffset;
                    decoderInfo.msgLen = msglen;
                    decoderInfo.dataStart = bufOffset + 4;
                    decoderInfoList.Add(decoderInfo);
                    totalMsgLen += msglen;
                    ptr += 4;

                    var nextBufOffset = bufOffset;

                    while (fin == false)
                    {
                        nextBufOffset = nextBufOffset + 4 + msglen;
                        if ((buf[nextBufOffset] & 0b11111111) != (0b10000000))
                            return;

                        fin = ((buf[nextBufOffset]) & 0b10000000) != 0;

                        nextBufOffset++;
                        var nextMsgLen = buf[nextBufOffset] - 128; // & 0111 1111

                        ptr++;
                        nextBufOffset++;

                        if (nextMsgLen < 125)
                        {
                            nextBufOffset++;
                            ptr++;
                        }
                        else if (nextMsgLen == 126)
                        {
                            ptr += 3;
                            nextMsgLen = BitConverter.ToUInt16(new byte[] { buf[nextBufOffset + 1], buf[nextBufOffset] }, 0);
                            nextBufOffset += 2;
                        }
                        else if (nextMsgLen == 127)
                        {
                            ptr += 9;
                            nextMsgLen = (int)BitConverter.ToUInt64(new byte[] { buf[nextBufOffset + 7], buf[nextBufOffset + 6], buf[nextBufOffset + 5], buf[nextBufOffset + 4], buf[nextBufOffset + 3], buf[nextBufOffset + 2], buf[nextBufOffset + 1], buf[nextBufOffset] }, 0);
                            nextBufOffset += 8;
                        }

                        var nextDecoderInfo = new Decoder();
                        nextDecoderInfo.msgLen = nextMsgLen;
                        nextDecoderInfo.maskStart = nextBufOffset;
                        nextDecoderInfo.dataStart = nextBufOffset + 4;
                        decoderInfoList.Add(nextDecoderInfo);
                        totalMsgLen += nextMsgLen;
                        ptr += 4;
                    }

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
                }

                fixed (byte* ptr1 = &decoded[0])
                {
                    var src = ptr1;
                    int opSequenceId = *(int*)src;
                    src += sizeof(int);
                    var message = (MessageType)(*src++);
                    core.Status status = default;

                    switch (message)
                    {
                        case MessageType.Upsert:
                        case MessageType.UpsertAsync:
                            if ((int)(dend - dcurr) < 2)
                                SendAndReset(ref d, ref dend);

                            dcurr += 10;

                            var keyPtr = src;
                            status = session.Upsert(ref serializer.ReadKeyByRef(ref src), ref serializer.ReadValueByRef(ref src));

                            WriteOpSeqId(ref opSequenceId, ref dcurr, (int)(dend - dcurr));
                            hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                            Write(ref status, ref dcurr, (int)(dend - dcurr));

                            int packetLen = (int)((dcurr - 10) - d);
                            CreateSendPacketHeader(ref d, packetLen);

                            ptr += totalMsgLen;
                            readHead = (int)(ptr - recvBufferPtr);
                            _origReadHead = readHead;

                            subscribeKVBroker.Publish(keyPtr);
                            break;

                        case MessageType.Read:
                        case MessageType.ReadAsync:
                            if ((int)(dend - dcurr) < 2 + maxSizeSettings.MaxOutputSize)
                                SendAndReset(ref d, ref dend);

                            dcurr += 10;

                            long ctx = ((long)message << 32) | (long)pendingSeqNo;
                            status = session.Read(ref serializer.ReadKeyByRef(ref src), ref serializer.ReadInputByRef(ref src),
                                ref serializer.AsRefOutput(dcurr + 6, (int)(dend - dcurr)), ctx, 0);

                            WriteOpSeqId(ref opSequenceId, ref dcurr, (int)(dend - dcurr));
                            hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                            Write(ref status, ref dcurr, (int)(dend - dcurr));

                            if (status == core.Status.PENDING)
                                Write(pendingSeqNo++, ref dcurr, (int)(dend - dcurr));
                            else if (status == core.Status.OK)
                                serializer.SkipOutput(ref dcurr);

                            packetLen = (int)((dcurr - 10) - d);
                            CreateSendPacketHeader(ref d, packetLen);

                            ptr += totalMsgLen;
                            readHead = (int)(ptr - recvBufferPtr);
                            _origReadHead = readHead;

                            break;

                        case MessageType.RMW:
                        case MessageType.RMWAsync:
                            if ((int)(dend - dcurr) < 2)
                                SendAndReset(ref d, ref dend);

                            dcurr += 10;

                            keyPtr = src;

                            ctx = ((long)message << 32) | (long)pendingSeqNo;
                            status = session.RMW(ref serializer.ReadKeyByRef(ref src), ref serializer.ReadInputByRef(ref src), ctx);

                            WriteOpSeqId(ref opSequenceId, ref dcurr, (int)(dend - dcurr));
                            hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                            Write(ref status, ref dcurr, (int)(dend - dcurr));
                            if (status == core.Status.PENDING)
                                Write(pendingSeqNo++, ref dcurr, (int)(dend - dcurr));

                            packetLen = (int)((dcurr - 10) - d);
                            CreateSendPacketHeader(ref d, packetLen);

                            ptr += totalMsgLen;
                            readHead = (int)(ptr - recvBufferPtr);
                            _origReadHead = readHead;

                            subscribeKVBroker.Publish(keyPtr);
                            break;

                        case MessageType.Delete:
                        case MessageType.DeleteAsync:
                            if ((int)(dend - dcurr) < 2)
                                SendAndReset(ref d, ref dend);

                            dcurr += 10;

                            keyPtr = src;

                            status = session.Delete(ref serializer.ReadKeyByRef(ref src));

                            WriteOpSeqId(ref opSequenceId, ref dcurr, (int)(dend - dcurr));
                            hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                            Write(ref status, ref dcurr, (int)(dend - dcurr));

                            packetLen = (int)((dcurr - 10) - d);
                            CreateSendPacketHeader(ref d, packetLen);

                            ptr += totalMsgLen;
                            readHead = (int)(ptr - recvBufferPtr);
                            _origReadHead = readHead;

                            subscribeKVBroker.Publish(keyPtr);
                            break;

                        case MessageType.SubscribeKV:
                            if ((int)(dend - dcurr) < 2 + maxSizeSettings.MaxOutputSize)
                                SendAndReset(ref d, ref dend);

                            dcurr += 10;

                            var keyStart = src;
                            ref Key key = ref serializer.ReadKeyByRef(ref src);

                            int sid = subscribeKVBroker.Subscribe(ref keyStart, this);
                            status = Status.PENDING;

                            WriteOpSeqId(ref opSequenceId, ref dcurr, (int)(dend - dcurr));
                            hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                            Write(ref status, ref dcurr, (int)(dend - dcurr));
                            Write(sid, ref dcurr, (int)(dend - dcurr));
                            serializer.Write(ref key, ref dcurr, (int)(dend - dcurr));

                            packetLen = (int)((dcurr - 10) - d);
                            CreateSendPacketHeader(ref d, packetLen);

                            ptr += totalMsgLen;
                            readHead = (int)(ptr - recvBufferPtr);
                            _origReadHead = readHead;

                            break;

                        case MessageType.PSubscribeKV:
                            if ((int)(dend - dcurr) < 2 + maxSizeSettings.MaxOutputSize)
                                SendAndReset(ref d, ref dend);

                            dcurr += 10;

                            keyStart = src;
                            key = ref serializer.ReadKeyByRef(ref src);

                            sid = subscribeKVBroker.PSubscribe(ref keyStart, this);
                            status = Status.PENDING;

                            WriteOpSeqId(ref opSequenceId, ref dcurr, (int)(dend - dcurr));
                            hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                            Write(ref status, ref dcurr, (int)(dend - dcurr));
                            Write(sid, ref dcurr, (int)(dend - dcurr));
                            serializer.Write(ref key, ref dcurr, (int)(dend - dcurr));

                            packetLen = (int)((dcurr - 10) - d);
                            CreateSendPacketHeader(ref d, packetLen);

                            ptr += totalMsgLen;
                            readHead = (int)(ptr - recvBufferPtr);
                            _origReadHead = readHead;

                            break;

                        default:
                            throw new NotImplementedException();
                    }
                }

            sendReply:
                // Send replies
                Send(d);
            }
        }

        public unsafe override void Publish(ref byte* keyPtr, int keyLength, int sid, bool prefix)
        {
            Input input = default;
            MessageType message = MessageType.SubscribeKV;
            if (prefix)
                message = MessageType.PSubscribeKV;

            GetResponseObject();

            ref Key key = ref serializer.ReadKeyByRef(ref keyPtr);

            byte* d = responseObject.obj.bufferPtr;
            var dend = d + responseObject.obj.buffer.Length;
            dcurr = d; // reserve space for size
            byte* outputDcurr;

            start = 0;

            if ((int)(dend - dcurr) < 10 + maxSizeSettings.MaxOutputSize)
                SendAndReset(ref d, ref dend);

            long ctx = ((long)message << 32) | (long)sid;

            if (prefix)
                outputDcurr = dcurr + 10 + 10 + keyLength;
            else
                outputDcurr = dcurr + 10 + 10;

            var status = session.Read(ref key, ref input, ref serializer.AsRefOutput(outputDcurr, (int)(dend - dcurr)), ctx, 0);

            if (status != Status.PENDING)
            {
                dcurr += 10;
                // Write six bytes (message | status | sid)
                int opSequenceId = 0;
                WriteOpSeqId(ref opSequenceId, ref dcurr, (int)(dend - dcurr));
                hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                Write(ref status, ref dcurr, (int)(dend - dcurr));
                Write(sid, ref dcurr, (int)(dend - dcurr));
                if (prefix)
                    serializer.Write(ref key, ref dcurr, (int)(dend - dcurr));

                if (status == Status.OK)
                    serializer.SkipOutput(ref dcurr);

                int packetLen = (int)((dcurr - 10) - d);
                CreateSendPacketHeader(ref d, packetLen);
            }

            // Send replies
            Send(d);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe bool WriteOpSeqId(ref int o, ref byte* dst, int length)
        {
            if (length < sizeof(int)) return false;
            *(int*)dst = o;
            dst += sizeof(int);
            return true;
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
            if ((int)(dcurr - d) > 0)
            {
                //Debug.WriteLine("RESP: [" + Encoding.ASCII.GetString(new Span<byte>(d, (int)(dcurr - d)).ToArray()) + "]\n\n");
                SendResponse((int)(d - responseObject.obj.bufferPtr), (int)(dcurr - d));
                responseObject.obj = null;
            }
        }
    }
}
