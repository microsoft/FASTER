using System;

namespace FASTER.core
{
    /// <summary>
    ///  Represents a entry that can be serialized directly onto FasterLog when enqueuing
    /// </summary>
    public interface ILogEnqueueEntry
    {
        /// <summary></summary>
        /// <returns> the size in bytes after serialization onto FasterLog</returns>
        public int SerializedLength { get; }

        /// <summary>
        /// Serialize the entry onto FasterLog.
        /// </summary>
        /// <param name="dest">Memory buffer of FasterLog to serialize onto. Guaranteed to have at least SerializedLength() many bytes</param>
        public void SerializeTo(Span<byte> dest);
    }
}