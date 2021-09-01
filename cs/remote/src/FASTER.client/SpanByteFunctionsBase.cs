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
    public class SpanByteFunctionsBase : CallbackFunctionsBase<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, byte>
    {
        /// <inheritdoc />
        public override void DeleteCompletionCallback(ref SpanByte key, byte ctx) { }
        /// <inheritdoc />
        public override void ReadCompletionCallback(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, byte ctx, Status status) { }
        /// <inheritdoc />
        public override void RMWCompletionCallback(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, byte ctx, Status status) { }
        /// <inheritdoc />
        public override void UpsertCompletionCallback(ref SpanByte key, ref SpanByte value, byte ctx) { }
        /// <inheritdoc />
        public override void SubscribeKVCallback(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, byte ctx, Status status) { }
        /// <inheritdoc/>
        public override void PublishCompletionCallback(ref SpanByte key, ref SpanByte value, byte ctx) { }
        /// <inheritdoc/>
        public override void SubscribeCallback(ref SpanByte key, ref SpanByte value, byte ctx) { }
    }
}
