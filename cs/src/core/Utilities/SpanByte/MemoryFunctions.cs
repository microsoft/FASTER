
using System;
using System.Buffers;

namespace FASTER.core
{
    /// <summary>
    /// IFunctions implementation for Memory&lt;T&gt; values, for blittable (unmanaged) type T
    /// </summary>
    public class MemoryFunctions<Key, T, Context> : FunctionsBase<Key, Memory<T>, Memory<T>, (IMemoryOwner<T>, int), Context>
        where T : unmanaged
    {
        readonly MemoryPool<T> memoryPool;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="memoryPool"></param>
        public MemoryFunctions(MemoryPool<T> memoryPool = default)
        {
            this.memoryPool = memoryPool ?? MemoryPool<T>.Shared;
        }

        ///<inheritdoc/>
        public override void SingleWriter(ref Key key, ref Memory<T> src, ref Memory<T> dst)
        {
            src.CopyTo(dst);
        }

        ///<inheritdoc/>
        public override bool ConcurrentWriter(ref Key key, ref Memory<T> src, ref Memory<T> dst)
        {
            // We can using the existing destination Memory<byte> if there is space, this 
            // extension method adjusts the length header and zeroes out the extra space 
            // on the FASTER log. The Memory<byte> reference's length itself is not updated
            // as it is a read-only field.
            if (!dst.ShrinkSerializedLength(src.Length)) return false;
            src.CopyTo(dst);
            return true;
        }

        ///<inheritdoc/>
        public override void SingleReader(ref Key key, ref Memory<T> input, ref Memory<T> value, ref (IMemoryOwner<T>, int) dst)
        {
            dst.Item1 = memoryPool.Rent(value.Length);
            dst.Item2 = value.Length;
            value.CopyTo(dst.Item1.Memory);
        }

        ///<inheritdoc/>
        public override void ConcurrentReader(ref Key key, ref Memory<T> input, ref Memory<T> value, ref (IMemoryOwner<T>, int) dst)
        {
            dst.Item1 = memoryPool.Rent(value.Length);
            dst.Item2 = value.Length;
            value.CopyTo(dst.Item1.Memory);
        }
    }
}