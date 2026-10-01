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

        public override unsafe int TryConsumeMessages(byte* req_buf, int bytesRead)
        {
            this.bytesRead = bytesRead;
            readHead = 0;
            while (TryReadMessages(out var offset))
            {
                if (!ProcessBatch(req_buf, bytesRead, offset)) break;
            }
            return readHead;
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

        const int OpcodeContinuation = 0x0;
        const int OpcodeText = 0x1;
        const int OpcodeBinary = 0x2;
        const int OpcodeClose = 0x8;
        const int OpcodePing = 0x9;
        const int OpcodePong = 0xA;

        /// <summary>
        /// Upper bound on the total size of a single (possibly fragmented) websocket message. Larger
        /// lengths are rejected outright, rather than being used to size an allocation.
        /// </summary>
        const int MaxMessageSize = 1 << 26;

        /// <summary>
        /// Parse the websocket frames making up one message, starting at <paramref name="offset"/>. Every
        /// frame header and payload length is validated against the bytes actually received, so that a
        /// malformed or oversized length can neither be read past the end of the receive buffer nor be
        /// narrowed into an out-of-range allocation.
        /// </summary>
        /// <returns>False if the message has not fully arrived yet, in which case the caller must wait for more data</returns>
        /// <exception cref="FormatException">The data received violates the websocket framing rules</exception>
        private bool TryParseFrames(byte* buf, int length, int offset, List<Decoder> frames, out int totalMsgLen, out int nextOffset, out int opcode)
        {
            totalMsgLen = 0;
            nextOffset = offset;
            opcode = OpcodeContinuation;

            bool fin = false, first = true;

            while (!fin)
            {
                // Every frame header is at least two bytes
                if (length - nextOffset < 2) return false;

                fin = (buf[nextOffset] & 0b10000000) != 0;
                int frameOpcode = buf[nextOffset] & 0b00001111;
                // "All frames sent from client to server have this [mask] bit set to 1"
                bool masked = (buf[nextOffset + 1] & 0b10000000) != 0;
                long msglen = buf[nextOffset + 1] & 0b01111111;
                nextOffset += 2;

                if (!masked)
                    throw new FormatException("Unmasked websocket frame received from client");

                if (msglen == 126)
                {
                    if (length - nextOffset < sizeof(ushort)) return false;
                    msglen = ((long)buf[nextOffset] << 8) | buf[nextOffset + 1];
                    nextOffset += sizeof(ushort);
                }
                else if (msglen == 127)
                {
                    if (length - nextOffset < sizeof(ulong)) return false;
                    ulong extended = 0;
                    for (int i = 0; i < sizeof(ulong); i++)
                        extended = (extended << 8) | buf[nextOffset + i];
                    if (extended > MaxMessageSize)
                        throw new FormatException($"Websocket payload length ({extended}) exceeds the maximum supported message size");
                    msglen = (long)extended;
                    nextOffset += sizeof(ulong);
                }

                if (frameOpcode >= OpcodeClose)
                {
                    // Control frames cannot be fragmented, carry at most 125 bytes, and we do not
                    // support them interleaved into a fragmented message
                    if (!fin || msglen > 125 || !first)
                        throw new FormatException("Malformed websocket control frame");
                }
                else if (first)
                {
                    if (frameOpcode != OpcodeText && frameOpcode != OpcodeBinary)
                        throw new FormatException($"Unsupported websocket opcode ({frameOpcode})");
                }
                else if (frameOpcode != OpcodeContinuation)
                    throw new FormatException("Expected websocket continuation frame");

                if (first) opcode = frameOpcode;
                first = false;

                totalMsgLen += (int)msglen;
                if (totalMsgLen > MaxMessageSize)
                    throw new FormatException("Websocket message exceeds the maximum supported message size");

                // Four bytes of masking key, followed by the payload
                if (length - nextOffset < sizeof(int) || length - nextOffset - sizeof(int) < msglen) return false;

                frames.Add(new Decoder { msgLen = (int)msglen, maskStart = nextOffset, dataStart = nextOffset + sizeof(int) });
                nextOffset += sizeof(int) + (int)msglen;
            }

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

            byte* d = networkSender.GetResponseObjectHead();
            var dend = networkSender.GetResponseObjectTail();
            dcurr = d; // reserve space for size
            byte[] decoded;
            List<Decoder> decoderInfoList = new();

            if (length - offset >= 3 && buf[offset] == 71 && buf[offset + 1] == 69 && buf[offset + 2] == 84)
            {
                // 1. Obtain the value of the "Sec-WebSocket-Key" request header without any leading or trailing whitespace
                // 2. Concatenate it with "258EAFA5-E914-47DA-95CA-C5AB0DC85B11" (a special GUID specified by RFC 6455)
                // 3. Compute SHA-1 and Base64 hash of the new value
                // 4. Write the hash back as the value of "Sec-WebSocket-Accept" response header in an HTTP response                

                //string s = Encoding.UTF8.GetString(buf, offset, length - offset);
                string s = Encoding.UTF8.GetString(new ReadOnlySpan<byte>((void*)(buf + offset), length - offset).ToArray());

                // Wait until the full HTTP request has arrived before replying to it
                if (!s.Contains("\r\n\r\n"))
                {
                    networkSender.ReturnResponseObject();
                    return false;
                }

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
                // Parse the (possibly fragmented) WebSocket message, validating every length field
                // against the bytes actually received before any of them is used.
                if (!TryParseFrames(buf, length, offset, decoderInfoList, out var totalMsgLen, out var nextBufOffset, out var opcode))
                {
                    // Message has not fully arrived yet; leave readHead untouched and wait for more data
                    networkSender.ReturnResponseObject();
                    return false;
                }

                if (opcode == OpcodeClose)
                {
                    networkSender.ReturnResponseObject();
                    readHead = nextBufOffset;
                    this.Dispose();
                    return false;
                }

                readHead = nextBufOffset;

                // Ping/pong carry no FASTER payload; consume them and move on
                if (opcode == OpcodePing || opcode == OpcodePong)
                {
                    networkSender.ReturnResponseObject();
                    return completeWSCommand;
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

            // The decoded message is [4 byte size][BatchHeader][messages...]
            if (decoded.Length < sizeof(int) + BatchHeader.Size)
            {
                networkSender.ReturnResponseObject();
                throw new FormatException("Truncated batch header in websocket request");
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
                var end = ptr1 + (decoded.Length - sizeof(int));
                ref var header = ref Unsafe.AsRef<BatchHeader>(src);
                int num = *(int*)(src + 4);
                src += BatchHeader.Size;
                Status status = default;

                for (msgnum = 0; msgnum < num; msgnum++)
                {
                    if (end - src < 1)
                        throw new FormatException("Truncated message header in websocket request");
                    var message = (MessageType)(*src++);

                    switch (message)
                    {
                        case MessageType.Upsert:
                        case MessageType.UpsertAsync:
                            if ((int)(dend - dcurr) < 2)
                                SendAndReset(ref d, ref dend);

                            var keyPtr = src;
                            status = session.Upsert(ref serializer.ReadKeyByRef(ref src, end), ref serializer.ReadValueByRef(ref src, end));

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
                            status = session.Read(ref serializer.ReadKeyByRef(ref src, end), ref serializer.ReadInputByRef(ref src, end),
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
                            status = session.RMW(ref serializer.ReadKeyByRef(ref src, end), ref serializer.ReadInputByRef(ref src, end), ctx);

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

                            status = session.Delete(ref serializer.ReadKeyByRef(ref src, end));

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
                            ref Key key = ref serializer.ReadKeyByRef(ref src, end);

                            var inputStart = src;
                            ref Input input = ref serializer.ReadInputByRef(ref src, end);

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
                            key = ref serializer.ReadKeyByRef(ref src, end);

                            inputStart = src;
                            input = ref serializer.ReadInputByRef(ref src, end);

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
                            key = ref serializer.ReadKeyByRef(ref src, end);
                            byte* valPtr = src;
                            ref Value val = ref serializer.ReadValueByRef(ref src, end);
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
                            serializer.ReadKeyByRef(ref src, end);

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
                            serializer.ReadKeyByRef(ref src, end);

                            sid = subscribeBroker.PSubscribe(ref keyStart, this);
                            status = Status.CreatePending();
                            hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                            Write(ref status, ref dcurr, (int)(dend - dcurr));
                            Write(sid, ref dcurr, (int)(dend - dcurr));
                            break;

                        default:
                            throw new FormatException($"Unsupported message type ({message}) in websocket request");
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
