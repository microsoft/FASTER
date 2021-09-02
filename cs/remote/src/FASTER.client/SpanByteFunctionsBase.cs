using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FASTER.client
{
    /// <summary>
    /// Base class for callback functions on SpanByte
    /// </summary>
    public class SpanByteFunctionsBase : ICallbackFunctions<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, byte>
    {
        /// <inheritdoc />
        public virtual void DeleteCompletionCallback(ref SpanByte key, byte ctx) { }
        /// <inheritdoc />
        public virtual void ReadCompletionCallback(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, byte ctx, Status status) { }
        /// <inheritdoc />
        public virtual void RMWCompletionCallback(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, byte ctx, Status status) { }
        /// <inheritdoc />
        public virtual void UpsertCompletionCallback(ref SpanByte key, ref SpanByte value, byte ctx) { }
        /// <inheritdoc />
        public virtual void SubscribeKVCallback(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, byte ctx, Status status) { }
        /// <inheritdoc/>
        public virtual void PublishCompletionCallback(ref SpanByte key, ref SpanByte value, byte ctx) { }
        /// <inheritdoc/>
        public virtual void SubscribeCallback(ref SpanByte key, ref SpanByte value, byte ctx) { }
    }
}
