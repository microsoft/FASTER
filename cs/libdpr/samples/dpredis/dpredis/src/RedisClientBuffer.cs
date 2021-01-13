using System;

namespace dpredis
{
    public class RedisClientBuffer
    {
        public const int MAX_BUFFER_SIZE = 1 << 20;
        
        private byte[] messageBuffer = new byte[MAX_BUFFER_SIZE];
        private int head = 0, commandCount = 0;

        public Span<byte> GetCurrentBytes() => new Span<byte>(messageBuffer, 0, head);

        public int CommandCount() => commandCount;

        public void Reset()
        {
            head = 0;
            commandCount = 0;
        }

        public bool TryAddSetCommand(ulong key, ulong value)
        {
            var newHead = head;
            newHead += MessageUtil.WriteRedisArrayHeader(3, messageBuffer, head);
            newHead += MessageUtil.WriteRedisBulkString("SET", messageBuffer, head);
            newHead += MessageUtil.WriteRedisBulkString(key, messageBuffer, head);
            newHead += MessageUtil.WriteRedisBulkString(value, messageBuffer, head);
            if (newHead == head)
                return false;

            head = newHead;
            commandCount++;
            return true;
        }

        public bool TryAddGetCommand(ulong key)
        {
            var newHead = head;
            newHead += MessageUtil.WriteRedisArrayHeader(2, messageBuffer, head);
            newHead += MessageUtil.WriteRedisBulkString("GET", messageBuffer, head);
            newHead += MessageUtil.WriteRedisBulkString(key, messageBuffer, head);
            if (newHead == head)
                return false;

            head = newHead;
            commandCount++;
            return true;
        }
    }
}