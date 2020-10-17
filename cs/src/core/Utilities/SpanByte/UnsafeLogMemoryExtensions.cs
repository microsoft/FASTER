
using System;

namespace FASTER.core
{
    public static class UnsafeLogMemoryExtensions
    {
        /// <summary>
        /// Helper to shrink the length of the in-place allocated buffer on
        /// FASTER hybrid log, pointed to by the given Memory&lt;byte&gt;.
        /// Zeroes out the extra space to retain log scan correctness.
        /// </summary>
        /// <param name="memory">Memory&lt;byte&gt; to shrink serialized length of</param>
        /// <param name="newLength">New length</param>
        /// <returns>True is adjustment was successful</returns>
        public static unsafe bool ShrinkSerializedLength(this Memory<byte> memory, int newLength)
        {
            if (newLength > memory.Length) return false;
            if (newLength < memory.Length)
            {
                fixed (byte* ptr = memory.Span)
                {
                    memory.Span.Slice(memory.Length).Fill(default);
                    *(int*)(ptr - sizeof(int)) = newLength;
                }
            }
            return true;
        }
    }
}