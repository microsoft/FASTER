using System;
using System.Collections.Generic;
using System.Diagnostics;
using FASTER.common;
using FASTER.core;

namespace FASTER.libdpr
{
    public enum DarqMessageType : byte
    {
        IN,
        OUT,
        SELF,
        COMPLETION,
        CHECKPOINT
    }

    public class DarqMessage : IDisposable
    {
        private DarqMessageType type;
        private long lsn, nextLsn;
        private byte[] message;
        private int messageSize;
        private SimpleObjectPool<DarqMessage> messagePool;
        private WorkerVersion wv;

        public DarqMessage(SimpleObjectPool<DarqMessage> messagePool)
        {
            message = new byte[1 << 20];
            this.messagePool = messagePool;
        }

        public void Dispose() => messagePool?.Return(this);

        public DarqMessageType GetMessageType() => type;

        public WorkerVersion WorkerVersion() => wv;

        public long GetLsn() => lsn;

        public long GetNextLsn() => nextLsn;

        public ReadOnlySpan<byte> GetMessageBody => new ReadOnlySpan<byte>(message, 0, messageSize);

        public void Reset(DarqMessageType type, long lsn, long nextLsn, WorkerVersion wv, ReadOnlySpan<byte> msg)
        {
            this.type = type;
            this.lsn = lsn;
            this.nextLsn = nextLsn;
            this.wv = wv;
            Debug.Assert(message.Length > msg.Length);
            msg.CopyTo(message);
            messageSize = msg.Length;
        }
    }

    public class StepRequest : IReadOnlySpanBatch, IDisposable
    {
        internal WorkerId me;
        internal List<long> steppedMessages;
        internal List<int> offsets;
        internal int size;
        internal byte[] serializationBuffer;
        private SimpleObjectPool<StepRequest> requestPool;

        public StepRequest(SimpleObjectPool<StepRequest> requestPool)
        {
            serializationBuffer = new byte[1 << 15];
            steppedMessages = new List<long>();
            offsets = new List<int>();
            this.requestPool = requestPool;
        }

        public void Dispose() => requestPool?.Return(this);

        internal void Reset(WorkerId me)
        {
            this.me = me;
            steppedMessages.Clear();
            offsets.Clear();
            size = 0;
        }

        public List<long> SteppedMessages() => steppedMessages;

        public int TotalEntries()
        {
            return offsets.Count;
        }

        public void Grow()
        {
            var oldBuffer = serializationBuffer;
            serializationBuffer = new byte[2 * oldBuffer.Length];
            Array.Copy(oldBuffer, serializationBuffer, oldBuffer.Length);
        }

        public ReadOnlySpan<byte> Get(int index)
        {
            return new Span<byte>(serializationBuffer, offsets[index],
                (index == (offsets.Count - 1) ? size : offsets[index + 1]) - offsets[index]);
        }
    }

    public struct StepRequestBuilder
    {
        private StepRequest request;

        public StepRequestBuilder(StepRequest toBuild, WorkerId me)
        {
            request = toBuild;
            request.Reset(me);
        }

        public StepRequestBuilder(StepRequest toBuild)
        {
            request = toBuild;
            request.Reset(WorkerId.INVALID);
        }

        public StepRequestBuilder MarkMessageConsumed(long lsn)
        {
            request.steppedMessages.Add(lsn);
            return this;
        }

        public unsafe StepRequestBuilder StartCheckpoint()
        {
            while (request.serializationBuffer.Length - request.size < sizeof(DarqMessageType))
                request.Grow();

            request.offsets.Add(request.size);
            fixed (byte* b = request.serializationBuffer)
            {
                var head = b + request.size;
                *(DarqMessageType*)head++ = DarqMessageType.CHECKPOINT;
                request.size = (int)(head - b);
            }

            return this;
        }

        public unsafe StepRequestBuilder AddOutMessage(WorkerId recipient, ReadOnlySpan<byte> message)
        {
            while (request.serializationBuffer.Length - request.size <
                   message.Length + sizeof(DarqMessageType) + sizeof(WorkerId))
                request.Grow();

            request.offsets.Add(request.size);
            fixed (byte* b = request.serializationBuffer)
            {
                var head = b + request.size;
                if (recipient.Equals(request.me))
                {
                    *(DarqMessageType*)head++ = DarqMessageType.IN;
                }
                else
                {
                    *(DarqMessageType*)head++ = DarqMessageType.OUT;
                    *(WorkerId*)head = recipient;
                    head += sizeof(WorkerId);
                }

                message.CopyTo(new Span<byte>(head, message.Length));
                head += message.Length;
                request.size = (int)(head - b);
            }

            return this;
        }

        public unsafe StepRequestBuilder AddOutMessage(WorkerId recipient, string message)
        {
            while (request.serializationBuffer.Length - request.size <
                   message.Length + sizeof(DarqMessageType) + sizeof(WorkerId) + sizeof(int))
                request.Grow();

            request.offsets.Add(request.size);
            fixed (byte* b = request.serializationBuffer)
            {
                var head = b + request.size;
                if (recipient.Equals(request.me))
                {
                    *(DarqMessageType*)head++ = DarqMessageType.IN;
                }
                else
                {
                    *(DarqMessageType*)head++ = DarqMessageType.OUT;
                    *(WorkerId*)head = recipient;
                    head += sizeof(WorkerId);
                }

                *(int*)head = message.Length;
                head += sizeof(int);

                fixed (char* m = message)
                {
                    for (var i = 0; i < message.Length; i++)
                        *head++ = (byte)m[i];
                }

                request.size = (int)(head - b);
            }

            return this;
        }

        private static unsafe void CopyToSpan(string s, Span<byte> span)
        {
            Debug.Assert(span.Length >= s.Length);
            fixed (char* c = s)
            {
                for (var i = 0; i < s.Length; i++)
                    span[i] = (byte)c[i];
            }
        }

        private static unsafe void PadWithBytes(Span<byte> span)
        {
            var pad = 0xC0FFEEDEADBEEF;
            fixed (byte* b = span)
            {
                var bend = b + span.Length;
                for (var head = (long*)b; head + 1 < bend; head++)
                    *head = pad;

                for (var head = span.Length / sizeof(long) * sizeof(long); head < span.Length; head++)
                    span[head] = 0xDB;
            }
        }

        public unsafe StepRequestBuilder AddSelfMessage(Span<byte> message)
        {
            while (request.serializationBuffer.Length - request.size <
                   message.Length + sizeof(DarqMessageType) + sizeof(WorkerId))
                request.Grow();

            request.offsets.Add(request.size);
            fixed (byte* b = request.serializationBuffer)
            {
                var head = b + request.size;
                *(DarqMessageType*)head++ = DarqMessageType.SELF;
                message.CopyTo(new Span<byte>(head, message.Length));
                head += message.Length;
                request.size = (int)(head - b);
            }

            return this;
        }

        public unsafe StepRequestBuilder AddSelfMessage(string message)
        {
            while (request.serializationBuffer.Length - request.size <
                   message.Length + sizeof(DarqMessageType) + sizeof(WorkerId) + sizeof(int))
                request.Grow();

            request.offsets.Add(request.size);
            fixed (byte* b = request.serializationBuffer)
            {
                var head = b + request.size;
                *(DarqMessageType*)head++ = DarqMessageType.SELF;

                *(int*)head = message.Length;
                head += sizeof(int);

                fixed (char* m = message)
                {
                    for (var i = 0; i < message.Length; i++)
                        *head++ = (byte)m[i];
                }

                request.size = (int)(head - b);
            }

            return this;
        }

        public unsafe StepRequest FinishStep()
        {
            // Step needs to do something at least
            // TODO(Tianyu): Is there a valid step where we may drop one of these requirements?
            if (request.steppedMessages.Count < 1 && request.offsets.Count == 0)
                throw new InvalidOperationException();

            while (request.serializationBuffer.Length - request.size <
                   sizeof(DarqMessageType) + sizeof(long) * request.steppedMessages.Count)
                request.Grow();

            request.offsets.Add(request.size);
            fixed (byte* b = request.serializationBuffer)
            {
                var head = b + request.size;
                *(DarqMessageType*)head++ = DarqMessageType.COMPLETION;
                foreach (var lsn in request.steppedMessages)
                {
                    *(long*)head = lsn;
                    head += sizeof(long);
                }

                request.size = (int)(head - b);
            }

            return request;
        }
    }
}