
using System;
using System.Buffers;

namespace FASTER.core
{
    /// <summary>
    /// IFunctions implementation for Memory&lt;byte&gt; values
    /// </summary>
    public class MemoryFunctions<Key> : FunctionsBase<Key, Memory<byte>, Memory<byte>, (IMemoryOwner<byte>, int), Empty>
    {
        readonly MemoryPool<byte> memoryPool;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="memoryPool"></param>
        public MemoryFunctions(MemoryPool<byte> memoryPool = default)
        {
            this.memoryPool = memoryPool ?? MemoryPool<byte>.Shared;
        }

        ///<inheritdoc/>
        public override void SingleWriter(ref Key key, ref Memory<byte> src, ref Memory<byte> dst)
        {
            src.CopyTo(dst);
        }

        ///<inheritdoc/>
        public override bool ConcurrentWriter(ref Key key, ref Memory<byte> src, ref Memory<byte> dst)
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
        public override void SingleReader(ref Key key, ref Memory<byte> input, ref Memory<byte> value, ref (IMemoryOwner<byte>, int) dst)
        {
            dst.Item1 = memoryPool.Rent(value.Length);
            dst.Item2 = value.Length;
            value.CopyTo(dst.Item1.Memory);
        }

        ///<inheritdoc/>
        public override void ConcurrentReader(ref Key key, ref Memory<byte> input, ref Memory<byte> value, ref (IMemoryOwner<byte>, int) dst)
        {
            dst.Item1 = memoryPool.Rent(value.Length);
            dst.Item2 = value.Length;
            value.CopyTo(dst.Item1.Memory);
        }
    }
}