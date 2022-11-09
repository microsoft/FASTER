using System;
using FASTER.core;

namespace FASTER.libdpr
{
    public unsafe struct SerializedDarqEntryBatch : IReadOnlySpanBatch
    {
        private byte* head;

        public static int ComputeSerializedSize(ReadOnlySpan<byte> message)
        {
            // For single messages -- insert in message type 
            return 2 * sizeof(int) + sizeof(byte) + message.Length;
        }
        
        public static int ComputeSerializedSize(IReadOnlySpanBatch original)
        {
            // No need to add additional message type for  batched interface
            var size = (original.TotalEntries() + 1) * sizeof(int);
            for (var i = 0; i < original.TotalEntries(); i++)
            {
                size += original.Get(i).Length;
            }
            return size;
        }

        public SerializedDarqEntryBatch(byte* head)
        {
            this.head = head;
        }

        public int TotalSize()
        {
            return ((int*) head)[TotalEntries()];
        }

        public int TotalEntries()
        {
            return *(int*) head;
        }

        public ReadOnlySpan<byte> Get(int index)
        {
            var offsetStart = index == 0 ? sizeof(int) * (TotalEntries() + 1)  : ((int*) head)[index];
            var offsetEnd = ((int*) head)[index + 1];

            return new ReadOnlySpan<byte>(head + offsetStart, offsetEnd - offsetStart);
        }

        public void SetContent(ReadOnlySpan<byte> message)
        {
            var writeHead = head;
            *(int*) writeHead = 1;
            writeHead += sizeof(int);

            *(int*) writeHead = sizeof(int) * 2 + sizeof(byte) + message.Length;
            writeHead += sizeof(int);
            
            *(DarqMessageType*) writeHead = DarqMessageType.IN;
            writeHead += sizeof(DarqMessageType);

            message.CopyTo(new Span<byte>(writeHead, message.Length));
        }
        
        public void SetContent(IReadOnlySpanBatch batch)
        {
            *(int*) head = batch.TotalEntries();

            for (var i = 0; i < batch.TotalEntries(); i++)
            {
                var entry = batch.Get(i);
                var offsetStart = i == 0 ? sizeof(int) * (TotalEntries() + 1) : ((int*) head)[i];
                ((int*) head)[i + 1] = offsetStart + entry.Length;

                entry.CopyTo(new Span<byte>(head + offsetStart, entry.Length));
            }
        }
    }
}