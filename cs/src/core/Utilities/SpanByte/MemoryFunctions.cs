
using System;
using System.Buffers;

namespace FASTER.core
{
    /// <summary>
    /// IFunctions implementation for Memory&lt;byte&gt;
    /// </summary>
    public class MemoryFunctions : FunctionsBase<Memory<byte>, Memory<byte>, Memory<byte>, (IMemoryOwner<byte>, int), Empty>
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
        public override void SingleWriter(ref Memory<byte> key, ref Memory<byte> src, ref Memory<byte> dst)
        {
            src.CopyTo(dst);
        }

        ///<inheritdoc/>
        public override bool ConcurrentWriter(ref Memory<byte> key, ref Memory<byte> src, ref Memory<byte> dst)
        {
            if (dst.Length < src.Length) return false;
            src.CopyTo(dst);
            return true;
        }

        ///<inheritdoc/>
        public override void SingleReader(ref Memory<byte> key, ref Memory<byte> input, ref Memory<byte> value, ref (IMemoryOwner<byte>, int) dst)
        {
            dst.Item1 = memoryPool.Rent(value.Length);
            dst.Item2 = value.Length;
            value.CopyTo(dst.Item1.Memory);
        }

        ///<inheritdoc/>
        public override void ConcurrentReader(ref Memory<byte> key, ref Memory<byte> input, ref Memory<byte> value, ref (IMemoryOwner<byte>, int) dst)
        {
            dst.Item1 = memoryPool.Rent(value.Length);
            dst.Item2 = value.Length;
            value.CopyTo(dst.Item1.Memory);
        }
    }
}