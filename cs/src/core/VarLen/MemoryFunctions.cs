
using System;
using System.Buffers;

namespace FASTER.core
{
    /// <summary>
    /// IFunctions base implementation for Memory&lt;T&gt; values, for blittable (unmanaged) type T
    /// </summary>
    public class MemoryFunctions<Key, T, Context> : FunctionsBase<Key, Memory<T>, Memory<T>, (IMemoryOwner<T>, int), Context>
        where T : unmanaged
    {
        readonly MemoryPool<T> memoryPool;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="memoryPool"></param>
        /// <param name="locking">Whether we lock values before concurrent operations (implemented using a spin lock on length header bit)</param>
        public MemoryFunctions(MemoryPool<T> memoryPool = default, bool locking = false) : base(locking)
        {
            this.memoryPool = memoryPool ?? MemoryPool<T>.Shared;
        }

        /// <inheritdoc/>
        public override void SingleWriter(ref Key key, ref Memory<T> src, ref Memory<T> dst)
        {
            src.CopyTo(dst);
        }

        /// <inheritdoc/>
        public override bool ConcurrentWriter(ref Key key, ref Memory<T> src, ref Memory<T> dst)
        {
            // We can write the source (src) data to the existing destination (dst) in-place, 
            // only if there is sufficient space
            if (dst.Length < src.Length || dst.IsMarkedReadOnly())
            {
                dst.MarkReadOnly();
                return false;
            }

            // Option 1: write the source data, leaving the destination size unchanged. You will need
            // to mange the actual space used by the value if you stop here.
            src.CopyTo(dst);

            // We can adjust the length header on the serialized log, if we wish to.
            // This method will also zero out the extra space to retain log scan correctness.
            dst.ShrinkSerializedLength(src.Length);
            return true;
        }

        /// <inheritdoc/>
        public override void SingleReader(ref Key key, ref Memory<T> input, ref Memory<T> value, ref (IMemoryOwner<T>, int) dst)
        {
            dst.Item1 = memoryPool.Rent(value.Length);
            dst.Item2 = value.Length;
            value.CopyTo(dst.Item1.Memory);
        }

        /// <inheritdoc/>
        public override void ConcurrentReader(ref Key key, ref Memory<T> input, ref Memory<T> value, ref (IMemoryOwner<T>, int) dst)
        {
            dst.Item1 = memoryPool.Rent(value.Length);
            dst.Item2 = value.Length;
            value.CopyTo(dst.Item1.Memory);
        }

        /// <inheritdoc/>
        public override void InitialUpdater(ref Key key, ref Memory<T> input, ref Memory<T> value)
        {
            input.CopyTo(value);
        }

        /// <inheritdoc/>
        public override void CopyUpdater(ref Key key, ref Memory<T> input, ref Memory<T> oldValue, ref Memory<T> newValue)
        {
            oldValue.CopyTo(newValue);
        }

        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref Key key, ref Memory<T> input, ref Memory<T> value)
        {
            // The default implementation of IPU simply writes input to destination, if there is space
            return ConcurrentWriter(ref key, ref input, ref value);
        }

        /// <inheritdoc />
        public override bool SupportsLocking => locking;

        /// <inheritdoc />
        public override void Lock(ref RecordInfo recordInfo, ref Key key, ref Memory<T> value, LockType lockType, ref long lockContext)
        {
            if (locking) value.SpinLock();
        }

        /// <inheritdoc />
        public override bool Unlock(ref RecordInfo recordInfo, ref Key key, ref Memory<T> value, LockType lockType, long lockContext)
        {
            if (locking) value.Unlock();
            return true;
        }
    }
}