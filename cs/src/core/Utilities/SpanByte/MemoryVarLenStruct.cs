
using System;

namespace FASTER.core
{
    /// <summary>
    /// IVariableLengthStruct implementation for Memory&lt;byte&gt;
    /// </summary>
    public class MemoryVarLenStruct : IVariableLengthStruct<Memory<byte>>
    {
        ///<inheritdoc/>
        public int GetInitialLength()
        {
            return 2 * sizeof(int);
        }

        ///<inheritdoc/>
        public int GetLength(ref Memory<byte> t)
        {
            return sizeof(int) + t.Length;
        }

        ///<inheritdoc/>
        public unsafe void Serialize(ref Memory<byte> source, void* destination)
        {
            *(int*)destination = source.Length;
            source.Span.CopyTo(new Span<byte>((byte*)destination + sizeof(int), source.Length));
        }

        [ThreadStatic]
        readonly UnmanagedMemoryManager<byte> manager = new UnmanagedMemoryManager<byte>();

        [ThreadStatic]
        Memory<byte>[] obj = new Memory<byte>[4];

        [ThreadStatic]
        int count;

        ///<inheritdoc/>
        public unsafe ref Memory<byte> AsRef(void* source)
        {
            manager.SetDestination((byte*)source + sizeof(int), *(int*)source);
            count = (count + 1) % 4;
            obj[count] = manager.Memory;
            return ref obj[count];
        }

        ///<inheritdoc/>
        public unsafe ref Memory<byte> AsRef(void* source, void* end)
        {
            int len = (int)end - (int)source - sizeof(int);
            *(int*)source = len;
            manager.SetDestination((byte*)source + sizeof(int), len);
            count = (count + 1) % 4;
            obj[count] = manager.Memory;
            return ref obj[count];
        }
    }
}