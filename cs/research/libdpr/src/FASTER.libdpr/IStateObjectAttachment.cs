using System;

namespace FASTER.libdpr
{
    /// <summary>
    /// A StateObjectAttachment is a piece of data structure that can be checkpointed and recovered with state object
    /// content when attached to a DprWorker. The content is guaranteed to be consistent with StateObject if
    /// modification of the attachment only happens within protected processing blocks in DprWorker. DprWorker
    /// guarantees that these functions are invoked single-threaded and will not interleave with protected processing
    /// blocks. 
    /// </summary>
    public interface IStateObjectAttachment
    {
        /// <summary></summary>
        /// <returns> The size of the attachment when serialized, in bytes </returns>
        int SerializedSize();

        /// <summary>
        /// Serializes the attachment to the given buffer, which is guaranteed to be at least as large as SerializedSize()
        /// </summary>
        /// <param name="buffer"> The buffer to serialize to </param>
        void SerializeTo(Span<byte> buffer);

        /// <summary>
        /// Recover attachment state from the given bytes
        /// </summary>
        /// <param name="serialized"> serialized bytes</param>
        void RecoverFrom(ReadOnlySpan<byte> serialized);
    }
}