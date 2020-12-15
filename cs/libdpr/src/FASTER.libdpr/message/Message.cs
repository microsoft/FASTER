using System;
using System.Runtime.InteropServices;

namespace FASTER.libdpr
{
    public interface AppendableMessage
    {
        void AddBytes(Span<byte> bytes);

        Span<byte> GetAdditionalBytes();
    }
    
    [StructLayout(LayoutKind.Explicit, Size = 48)]
    public unsafe struct DprMessage
    {
        public const int HeaderSize = 48;
        [FieldOffset(0)]
        public fixed byte data[HeaderSize];
        [FieldOffset(0)]
        public Guid sessionId;
        [FieldOffset(16)]
        public long version;
        [FieldOffset(24)]
        public long worldLine;
        [FieldOffset(32)]
        public long serialNum;
        [FieldOffset(40)]
        public int numDeps;
        [FieldOffset(44)]
        public DprStatus ret;
        [FieldOffset(48)]
        public fixed byte deps[0];
    }
}