// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using FASTER.common;
using FASTER.core;

namespace FASTER.server
{
    internal unsafe sealed class BinaryServerSession<Key, Value, Input, Output, Functions, ParameterSerializer>
        : FasterKVServerSessionBase<Key, Value, Input, Output, Functions, ParameterSerializer>
        where Functions : IFunctions<Key, Value, Input, Output, long>
        where ParameterSerializer : IServerSerializer<Key, Value, Input, Output>
    {
        readonly HeaderReaderWriter hrw;
        int readHead;

        int seqNo, pendingSeqNo, msgnum, start;
        byte* dcurr;

        readonly SubscribeKVBroker<Key, Value, Input, IKeyInputSerializer<Key, Input>> subscribeKVBroker;
        readonly SubscribeBroker<Key, Value, IKeySerializer<Key>> subscribeBroker;

        public BinaryServerSession(
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
            if(this.networkSender.GetMaxSizeSettings.MaxOutputSize < sizeof(int))
                this.networkSender.GetMaxSizeSettings.MaxOutputSize = sizeof(int);
        }

        public override unsafe int TryConsumeMessages(byte* req_buf, int bytesReceived)
        {
            while (TryReadMessages(req_buf, out var offset))
                ProcessBatch(req_buf, offset);

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

        public override void CompleteRead(ref Output output, long ctx, Status status)
        {
            byte* d = networkSender.GetResponseObjectHead();
            var dend = networkSender.GetResponseObjectTail();

            if ((int)(dend - dcurr) < 7 + networkSender.GetMaxSizeSettings.MaxOutputSize)
                SendAndReset(ref d, ref dend);

            hrw.Write(MessageType.PendingResult, ref dcurr, (int)(dend - dcurr));
            hrw.Write((MessageType)(ctx >> 32), ref dcurr, (int)(dend - dcurr));
            Write((int)(ctx & 0xffffffff), ref dcurr, (int)(dend - dcurr));
            Write(ref status, ref dcurr, (int)(dend - dcurr));
            if (status != Status.NOTFOUND)
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
            if (status == Status.OK || status == Status.NOTFOUND)
                serializer.Write(ref output, ref dcurr, (int)(dend - dcurr));
            msgnum++;
        }

        private bool TryReadMessages(byte* buf, out int offset)
        {
            offset = default;

            var bytesAvailable = bytesRead - readHead;
            // Need to at least have read off of size field on the message
            if (bytesAvailable < sizeof(int)) return false;

            // MSB is 1 to indicate binary protocol
            var size = -(*(int*)buf);

            // Not all of the message has arrived
            if (bytesAvailable < size + sizeof(int)) return false;
            offset = readHead + sizeof(int);

            // Consume this message and the header
            readHead += size + sizeof(int);
            return true;
        }

        private unsafe void ProcessBatch(byte* buf, int offset)
        {
            networkSender.GetResponseObject();

            //fixed (byte* b = &buf[offset])
            byte* b = buf + offset;
            byte* d = networkSender.GetResponseObjectHead();
            var dend = networkSender.GetResponseObjectTail();
            dcurr = d + sizeof(int); // reserve space for size
            int origPendingSeqNo = pendingSeqNo;

            var src = b;
            ref var header = ref Unsafe.AsRef<BatchHeader>(src);
            var num = header.NumMessages;
            src += BatchHeader.Size;
            Status status = default;

            dcurr += BatchHeader.Size;
            start = 0;
            msgnum = 0;

            for (msgnum = 0; msgnum < num; msgnum++)
            {
                var message = (MessageType)(*src++);
                var serialNum = hrw.ReadSerialNum(ref src);
                switch (message)
                {
                    case MessageType.Upsert:
                    case MessageType.UpsertAsync:
                        if ((int)(dend - dcurr) < 2)
                            SendAndReset(ref d, ref dend);

                        var keyPtr = src;
                        status = session.Upsert(ref serializer.ReadKeyByRef(ref src), ref serializer.ReadValueByRef(ref src), serialNo: serialNum);

                        hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                        Write(ref status, ref dcurr, (int)(dend - dcurr));

                        subscribeKVBroker?.Publish(keyPtr);
                        break;

                    case MessageType.Read:
                    case MessageType.ReadAsync:
                        if ((int)(dend - dcurr) < 2 + networkSender.GetMaxSizeSettings.MaxOutputSize)
                            SendAndReset(ref d, ref dend);

                        long ctx = ((long)message << 32) | (long)pendingSeqNo;
                        status = session.Read(ref serializer.ReadKeyByRef(ref src), ref serializer.ReadInputByRef(ref src),
                            ref serializer.AsRefOutput(dcurr + 2, (int)(dend - dcurr)), ctx, serialNum);

                        hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                        Write(ref status, ref dcurr, (int)(dend - dcurr));

                        if (status == Status.PENDING)
                            Write(pendingSeqNo++, ref dcurr, (int)(dend - dcurr));
                        else if (status == Status.OK)
                            serializer.SkipOutput(ref dcurr);
                        break;

                    case MessageType.RMW:
                    case MessageType.RMWAsync:
                        if ((int)(dend - dcurr) < 2 + networkSender.GetMaxSizeSettings.MaxOutputSize)
                            SendAndReset(ref d, ref dend);

                        keyPtr = src;

                        ctx = ((long)message << 32) | (long)pendingSeqNo;
                        status = session.RMW(ref serializer.ReadKeyByRef(ref src), ref serializer.ReadInputByRef(ref src),
                            ref serializer.AsRefOutput(dcurr + 2, (int)(dend - dcurr)), ctx, serialNum);

                        hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                        Write(ref status, ref dcurr, (int)(dend - dcurr));
                        if (status == Status.PENDING)
                            Write(pendingSeqNo++, ref dcurr, (int)(dend - dcurr));
                        else if (status == Status.OK || status == Status.NOTFOUND)
                            serializer.SkipOutput(ref dcurr);

                        subscribeKVBroker?.Publish(keyPtr);
                        break;

                    case MessageType.Delete:
                    case MessageType.DeleteAsync:
                        if ((int)(dend - dcurr) < 2)
                            SendAndReset(ref d, ref dend);

                        keyPtr = src;
                        status = session.Delete(ref serializer.ReadKeyByRef(ref src), serialNo: serialNum);

                        hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                        Write(ref status, ref dcurr, (int)(dend - dcurr));

                        subscribeKVBroker?.Publish(keyPtr);
                        break;

                    default:
                        if (!HandlePubSub(message, ref src, ref d, ref dend)) throw new NotImplementedException();
                        break;
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
            var dcurr = d + sizeof(int); // reserve space for size
            byte* outputDcurr;

            dcurr += BatchHeader.Size;

            long ctx = ((long)message << 32) | (long)sid;

            if (prefix)
                outputDcurr = dcurr + 6 + keyLength;
            else
                outputDcurr = dcurr + 6;

            var status = Status.OK;
            if (valPtr == null)
                status = session.Read(ref key, ref serializer.ReadInputByRef(ref inputPtr), ref serializer.AsRefOutput(outputDcurr, (int)(dend - dcurr)), ctx, 0);

            if (status != Status.PENDING)
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
                else if (status == Status.OK)
                    serializer.SkipOutput(ref dcurr);
            }
            else
            {
                throw new Exception("Pending reads not supported with pub/sub");
            }

            // Send replies
            var dstart = d + sizeof(int);
            Unsafe.AsRef<BatchHeader>(dstart).NumMessages = 1;
            Unsafe.AsRef<BatchHeader>(dstart).SeqNo = 0;
            int payloadSize = (int)(dcurr - d);
            // Set packet size in header
            *(int*)networkSender.GetResponseObjectHead()= -(payloadSize - sizeof(int));
            networkSender.SendResponse(0, payloadSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe bool Write(ref Status s, ref byte* dst, int length)
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
            networkSender.GetResponseObject();
            d = networkSender.GetResponseObjectHead();
            dend = networkSender.GetResponseObjectTail();            
            dcurr = d + sizeof(int);
            start = msgnum;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Send(byte* d)
        {
            var dstart = d + sizeof(int);
            Unsafe.AsRef<BatchHeader>(dstart).NumMessages = msgnum - start;
            Unsafe.AsRef<BatchHeader>(dstart).SeqNo = seqNo++;
            int payloadSize = (int)(dcurr - d);
            // Set packet size in header
            *(int*)networkSender.GetResponseObjectHead() = -(payloadSize - sizeof(int));
            networkSender.SendResponse(0, payloadSize);
        }

        private bool HandlePubSub(MessageType message, ref byte* src, ref byte* d, ref byte* dend)
        {
            switch (message)
            {
                case MessageType.SubscribeKV:
                    if (subscribeKVBroker == null) return false;

                    if ((int)(dend - dcurr) < 2 + networkSender.GetMaxSizeSettings.MaxOutputSize)
                        SendAndReset(ref d, ref dend);

                    var keyStart = src;
                    serializer.ReadKeyByRef(ref src);

                    var inputStart = src;
                    serializer.ReadInputByRef(ref src);

                    int sid = subscribeKVBroker.Subscribe(ref keyStart, ref inputStart, this);
                    var status = Status.PENDING;
                    hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                    Write(ref status, ref dcurr, (int)(dend - dcurr));
                    Write(sid, ref dcurr, (int)(dend - dcurr));
                    break;

                case MessageType.PSubscribeKV:
                    if (subscribeKVBroker == null) return false;

                    if ((int)(dend - dcurr) < 2 + networkSender.GetMaxSizeSettings.MaxOutputSize)
                        SendAndReset(ref d, ref dend);

                    if (subscribeKVBroker == null)
                        break;

                    keyStart = src;
                    serializer.ReadKeyByRef(ref src);

                    inputStart = src;
                    serializer.ReadInputByRef(ref src);

                    sid = subscribeKVBroker.PSubscribe(ref keyStart, ref inputStart, this);
                    status = Status.PENDING;
                    hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                    Write(ref status, ref dcurr, (int)(dend - dcurr));
                    Write(sid, ref dcurr, (int)(dend - dcurr));
                    break;

                case MessageType.Publish:
                    if (subscribeBroker == null) return false;

                    if ((int)(dend - dcurr) < 2)
                        SendAndReset(ref d, ref dend);

                    var keyPtr = src;
                    ref Key key = ref serializer.ReadKeyByRef(ref src);
                    byte* valPtr = src;
                    ref Value val = ref serializer.ReadValueByRef(ref src);
                    int valueLength = (int)(src - valPtr);

                    status = Status.OK;
                    hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                    Write(ref status, ref dcurr, (int)(dend - dcurr));

                    if (subscribeBroker != null)
                        subscribeBroker.Publish(keyPtr, valPtr, valueLength);
                    break;

                case MessageType.Subscribe:
                    if (subscribeBroker == null) return false;

                    if ((int)(dend - dcurr) < 2 + networkSender.GetMaxSizeSettings.MaxOutputSize)
                        SendAndReset(ref d, ref dend);

                    keyStart = src;
                    serializer.ReadKeyByRef(ref src);

                    sid = subscribeBroker.Subscribe(ref keyStart, this);
                    status = Status.PENDING;
                    hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                    Write(ref status, ref dcurr, (int)(dend - dcurr));
                    Write(sid, ref dcurr, (int)(dend - dcurr));
                    break;

                case MessageType.PSubscribe:
                    if (subscribeBroker == null) return false;

                    if ((int)(dend - dcurr) < 2 + networkSender.GetMaxSizeSettings.MaxOutputSize)
                        SendAndReset(ref d, ref dend);

                    keyStart = src;
                    serializer.ReadKeyByRef(ref src);

                    sid = subscribeBroker.PSubscribe(ref keyStart, this);
                    status = Status.PENDING;
                    hrw.Write(message, ref dcurr, (int)(dend - dcurr));
                    Write(ref status, ref dcurr, (int)(dend - dcurr));
                    Write(sid, ref dcurr, (int)(dend - dcurr));
                    break;

                default:
                    return false;
            }
            return true;
        }

        public override void Dispose()
        {
            subscribeBroker?.RemoveSubscription(this);
            subscribeKVBroker?.RemoveSubscription(this);
        }
    }
}
