using System;
using System.Collections.Generic;
using System.Diagnostics;
using FASTER.common;
using FASTER.core;

namespace FASTER.libdpr
{
    /// <summary>
    /// DARQ Message Type
    /// </summary>
    public enum DarqMessageType : byte
    {
        /// <summary></summary>
        IN,
        /// <summary></summary>
        OUT,
        /// <summary></summary>
        SELF,
        /// <summary></summary>
        COMPLETION,
        /// <summary></summary>
        CHECKPOINT
    }

    /// <summary>
    /// DARQ Message 
    /// </summary>
    public class DarqMessage : IDisposable
    {
        private DarqMessageType type;
        private long lsn, nextLsn;
        private byte[] message;
        private int messageSize;
        private SimpleObjectPool<DarqMessage> messagePool;
        private WorkerVersion wv;

        /// <summary>
        /// Create a new DarqMessage object
        /// </summary>
        /// <param name="messagePool"> (optional) the object pool this message should be returned to on disposal </param>
        public DarqMessage(SimpleObjectPool<DarqMessage> messagePool = null)
        {
            message = new byte[1 << 20];
            this.messagePool = messagePool;
        }

        /// <inheritdoc/>
        public void Dispose() => messagePool?.Return(this);

        /// <summary></summary>
        /// <returns>Type of message</returns>
        public DarqMessageType GetMessageType() => type;

        /// <summary></summary>
        /// <returns>WorkerVersion this message is associated with (for speculative execution)</returns>
        public WorkerVersion WorkerVersion() => wv;

        /// <summary></summary>
        /// <returns>LSN of the message</returns>
        public long GetLsn() => lsn;

        /// <summary></summary>
        /// <returns>Lower bound for the LSN of the immediate next message in DARQ</returns>
        public long GetNextLsn() => nextLsn;

        /// <summary></summary>
        /// <returns>Get the message bytes</returns>
        public ReadOnlySpan<byte> GetMessageBody() => new(message, 0, messageSize);

        /// <summary>
        /// Reset this message to hold supplied values instead
        /// </summary>
        /// <param name="type"></param>
        /// <param name="lsn"></param>
        /// <param name="nextLsn"></param>
        /// <param name="wv"></param>
        /// <param name="msg"></param>
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

    /// <summary>
    /// StepRequests represents a DARQ step
    /// </summary>
    public class StepRequest : IReadOnlySpanBatch, IDisposable
    {
        internal WorkerId me;
        internal List<long> consumedMessages;
        internal List<int> offsets;
        internal int size;
        internal byte[] serializationBuffer;
        private SimpleObjectPool<StepRequest> requestPool;

        /// <summary>
        /// Create a new StepRequest object.
        /// </summary>
        /// <param name="requestPool"> (optional) the object pool this request should be returned to on disposal </param>
        public StepRequest(SimpleObjectPool<StepRequest> requestPool)
        {
            serializationBuffer = new byte[1 << 15];
            consumedMessages = new List<long>();
            offsets = new List<int>();
            this.requestPool = requestPool;
        }

        /// <inheritdoc/>
        public void Dispose() => requestPool?.Return(this);

        internal void Reset(WorkerId me)
        {
            this.me = me;
            consumedMessages.Clear();
            offsets.Clear();
            size = 0;
        }

        /// <summary></summary>
        /// <returns>list of messages consumed in this step</returns>
        public List<long> ConsumedMessages() => consumedMessages;

        /// <inheritdoc/>
        public int TotalEntries()
        {
            return offsets.Count;
        }

        /// <summary>
        /// Grow the underlying serialization buffer to be double of its original size, in case the step no longer fits.
        /// </summary>
        public void Grow()
        {
            var oldBuffer = serializationBuffer;
            serializationBuffer = new byte[2 * oldBuffer.Length];
            Array.Copy(oldBuffer, serializationBuffer, oldBuffer.Length);
        }

        /// <inheritdoc/>
        public ReadOnlySpan<byte> Get(int index)
        {
            return new Span<byte>(serializationBuffer, offsets[index],
                (index == (offsets.Count - 1) ? size : offsets[index + 1]) - offsets[index]);
        }
    }

    /// <summary>
    /// Builder to populate StepRequest
    /// </summary>
    public struct StepRequestBuilder
    {
        private StepRequest request;

        /// <summary>
        /// Constructs a new StepRequestBuilder
        /// </summary>
        /// <param name="toBuild"> the StepRequest object to populate </param>
        /// <param name="me"> ID of the DARQ instance the step is for </param>
        public StepRequestBuilder(StepRequest toBuild, WorkerId me)
        {
            request = toBuild;
            request.Reset(me);
        }

        /// <summary>
        /// Mark a message as consumed by this step 
        /// </summary>
        /// <param name="lsn"> LSN of the consumed message </param>
        /// <returns>self-reference for chaining</returns>
        public StepRequestBuilder MarkMessageConsumed(long lsn)
        {
            request.consumedMessages.Add(lsn);
            return this;
        }
        
        /// <summary>
        /// Consume all IN/SELF messages until the LSN (inclusive). Users are responsible for handling any concurrent
        /// stepping that may occur (this message does not participate in normal validation for duplicate consumption)
        /// and adding new SELF messages that replace the consumed ones. 
        /// </summary>
        /// <param name="lsn"> LSN to consume until </param>
        /// <returns> self-reference for chaining </returns>
        public unsafe StepRequestBuilder ConsumeUntil(long lsn)
        {
            while (request.serializationBuffer.Length - request.size < sizeof(DarqMessageType) + sizeof(long))
                request.Grow();

            request.offsets.Add(request.size);
            fixed (byte* b = request.serializationBuffer)
            {
                var head = b + request.size;
                *(DarqMessageType*)head++ = DarqMessageType.CHECKPOINT;
                *(long*)head = lsn;
                request.size = (int)(head - b);
            }

            return this;
        }

        /// <summary>
        /// Add an out message to this step.
        /// </summary>
        /// <param name="recipient">Intended recipient</param>
        /// <param name="message">message body, in bytes</param>
        /// <returns> self-reference for chaining </returns>
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

        /// <summary>
        /// Add a self message to this step.
        /// </summary>
        /// <param name="message">message body, as bytes</param>
        /// <returns> self-reference for chaining </returns>
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

        /// <summary>
        /// Finishes a step for submission
        /// </summary>
        /// <returns> composed step object </returns>
        public unsafe StepRequest FinishStep()
        {
            // Step needs to do something at least
            // TODO(Tianyu): Is there a valid step where we may drop one of these requirements?
            if (request.consumedMessages.Count < 1 && request.offsets.Count == 0)
                throw new FasterException("Empty step detected");

            while (request.serializationBuffer.Length - request.size <
                   sizeof(DarqMessageType) + sizeof(long) * request.consumedMessages.Count)
                request.Grow();

            request.offsets.Add(request.size);
            fixed (byte* b = request.serializationBuffer)
            {
                var head = b + request.size;
                *(DarqMessageType*)head++ = DarqMessageType.COMPLETION;
                foreach (var lsn in request.consumedMessages)
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