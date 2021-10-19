
using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// Extensions for use of Memory in FASTER log
    /// </summary>
    public static class UnsafeLogMemoryExtensions
    {
        /// <summary>
        /// Helper to shrink the length of the in-place allocated buffer on
        /// FASTER hybrid log, pointed to by the given Memory&lt;T&gt;.
        /// Zeroes out the extra space to retain log scan correctness.
        /// </summary>
        /// <param name="memory">Memory&lt;byte&gt; to shrink serialized length of</param>
        /// <param name="newLength">New length</param>
        /// <returns>True is adjustment was successful</returns>
        public static unsafe bool ShrinkSerializedLength<T>(this Memory<T> memory, int newLength)
            where T : unmanaged
        {
            if (newLength > memory.Length) return false;
            if (newLength < memory.Length)
            {
                fixed (T* ptr = memory.Span)
                {
                    memory.Span.Slice(newLength).Fill(default);
                    *(int*)((byte*)ptr - sizeof(int)) = newLength*sizeof(T);
                }
            }
            return true;
        }
    }
}