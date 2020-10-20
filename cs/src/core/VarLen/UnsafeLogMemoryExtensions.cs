
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

        /// <summary>
        /// Lock Memory serialized on log, using bits from the length header
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="memory"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe void SpinLock<T>(this Memory<T> memory) where T : unmanaged
        {
            var ptr = Unsafe.AsPointer(ref memory.Span[0]);
            IntLocker.SpinLock(ref Unsafe.AsRef<int>((byte*)ptr - sizeof(int)));
        }

        /// <summary>
        /// Unlock Memory serialized on log, using bits from the length header
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="memory"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe void Unlock<T>(this Memory<T> memory) where T : unmanaged
        {
            var ptr = Unsafe.AsPointer(ref memory.Span[0]);
            IntLocker.Unlock(ref Unsafe.AsRef<int>((byte*)ptr - sizeof(int)));
        }

        /// <summary>
        /// Mark Memory serialized on log as read-only, using bits from the length header
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="memory"></param>
        public static unsafe void MarkReadOnly<T>(this Memory<T> memory) where T : unmanaged
        {
            var ptr = Unsafe.AsPointer(ref memory.Span[0]);
            IntLocker.Mark(ref Unsafe.AsRef<int>((byte*)ptr - sizeof(int)));
        }

        /// <summary>
        /// Lock Memory serialized on log, using bits from the length header
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="memory"></param>
        public static unsafe bool IsMarkedReadOnly<T>(this Memory<T> memory) where T : unmanaged
        {
            var ptr = Unsafe.AsPointer(ref memory.Span[0]);
            return IntLocker.IsMarked(ref Unsafe.AsRef<int>((byte*)ptr - sizeof(int)));
        }


    }
}