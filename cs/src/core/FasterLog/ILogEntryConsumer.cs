using System;

namespace FASTER.core
{
    /// <summary>
    /// Consumes a FasterLog entry without copying 
    /// </summary>
    public interface ILogEntryConsumer
    {
        /// <summary>
        /// Consumes the given entry.
        /// </summary>
        /// <param name="entry"> the entry to consume </param>
        /// <param name="currentAddress"> address of the consumed entry </param>
        /// <param name="nextAddress"> (predicted) address of the next entry </param>
        public void Consume(ReadOnlySpan<byte> entry, long currentAddress, long nextAddress);
    }

    /// <summary>
    /// Consumes FasterLog entries in bulk (raw data) without copying 
    /// </summary>
    public interface IBulkLogEntryConsumer
    {
        /// <summary>
        /// Consumes the given bulk entries (raw data).
        /// </summary>
        /// <param name="payloadPtr"></param>
        /// <param name="payloadLength"></param>
        /// <param name="currentAddress"> address of the consumed entry </param>
        /// <param name="nextAddress"> (predicted) address of the next entry </param>
        unsafe void Consume(byte* payloadPtr, int payloadLength, long currentAddress, long nextAddress);
    }

}