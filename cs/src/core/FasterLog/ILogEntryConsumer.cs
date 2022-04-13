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
}