using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;

namespace FASTER.benchmark
{
    public static class SocketIoUtil
    {
        public static bool ReceiveFully(this Socket clientSocket, byte[] buffer, int numBytes,
            SocketFlags flags = SocketFlags.None)
        {
            Debug.Assert(numBytes <= buffer.Length);
            var totalReceived = 0;
            do
            {
                var received = clientSocket.Receive(buffer, totalReceived, numBytes - totalReceived, flags);
                if (received == 0) return false;
                totalReceived += received;
            } while (totalReceived < numBytes);

            return true;
        }

        public static void SendFully(this Socket clientSocket, byte[] buffer, int offset, int size,
            SocketFlags flags = SocketFlags.None)
        {
            Debug.Assert(offset >= 0 && offset < buffer.Length && offset + size <= buffer.Length);
            var totalSent = 0;
            do
            {
                var sent = clientSocket.Send(buffer, offset + totalSent, size - totalSent, flags);
                totalSent += sent;
            } while (totalSent < size);
        }
    }

    [Serializable]
    public class BenchmarkMonitorMessage
    {
        public int type;
        public object content;

        public static BenchmarkMonitorMessage CreateInfoMessage(string logMessage)
        {
            return new BenchmarkMonitorMessage {type = 0, content = logMessage};
        }

        public static BenchmarkMonitorMessage CreateControlMessage(object body)
        {
            return new BenchmarkMonitorMessage {type = 1, content = body};

        }
    }
    
    
    public static class BenchmarkMessageUtil
    {
        private static BlockingCollection<byte[]> buffers;

        static BenchmarkMessageUtil()
        {
            buffers = new BlockingCollection<byte[]>();
            for (var i = 0; i < 100; i++)
                buffers.Add(new byte[1 << 22]);
        }
        
        public static BenchmarkMonitorMessage ReceiveBenchmarkMessage(this Socket clientSocket)
        {
            var buf = buffers.Take();
            if (!clientSocket.ReceiveFully(buf, sizeof(int)))
            {
                buffers.Add(buf);
                return null;
            }
            var type = BitConverter.ToInt32(buf, 0);
            clientSocket.ReceiveFully(buf, sizeof(int));
            var configSize = BitConverter.ToInt32(buf, 0);
            clientSocket.ReceiveFully(buf, configSize);
            var deserializer = new BinaryFormatter();
            using var s = new MemoryStream(buf);
            var content = deserializer.Deserialize(s);
            
            buffers.Add(buf);
            return type == 0 ? BenchmarkMonitorMessage.CreateInfoMessage((string) content) : BenchmarkMonitorMessage.CreateControlMessage(content);
        }

        public static void SendBenchmarkInfoMessage(this Socket clientSocket, object message)
        {
            var buf = buffers.Take();
            var serializer = new BinaryFormatter();
            using var s = new MemoryStream(buf);
            serializer.Serialize(s, message);
            var bytes = s.ToArray();

            clientSocket.SendFully(BitConverter.GetBytes(0), 0, sizeof(int));
            clientSocket.SendFully(BitConverter.GetBytes(bytes.Length), 0, sizeof(int));
            clientSocket.SendFully(bytes, 0, bytes.Length);
            buffers.Add(buf);
        }

        public static void SendBenchmarkControlMessage(this Socket clientSocket, object message)
        {
            var buf = buffers.Take();
            var serializer = new BinaryFormatter();
            using var s = new MemoryStream(buf);
            serializer.Serialize(s, message);
            var bytes = s.ToArray();

            clientSocket.SendFully(BitConverter.GetBytes(1), 0, sizeof(int));
            clientSocket.SendFully(BitConverter.GetBytes(bytes.Length), 0, sizeof(int));
            clientSocket.SendFully(bytes, 0, bytes.Length);
            buffers.Add(buf);
        }
    }
}