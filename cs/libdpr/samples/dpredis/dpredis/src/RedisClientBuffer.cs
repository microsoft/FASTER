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
            newHead += MessageUtil.WriteRedisArrayHeader(3, messageBuffer, newHead);
            newHead += MessageUtil.WriteRedisBulkString("SET", messageBuffer, newHead);
            newHead += MessageUtil.WriteRedisBulkString(key, messageBuffer, newHead);
            newHead += MessageUtil.WriteRedisBulkString(value, messageBuffer, newHead);
            if (newHead == head)
                return false;

            head = newHead;
            commandCount++;
            return true;
        }

        public bool TryAddGetCommand(ulong key)
        {
            var newHead = head;
            newHead += MessageUtil.WriteRedisArrayHeader(2, messageBuffer, newHead);
            newHead += MessageUtil.WriteRedisBulkString("GET", messageBuffer, newHead);
            newHead += MessageUtil.WriteRedisBulkString(key, messageBuffer, newHead);
            if (newHead == head)
                return false;

            head = newHead;
            commandCount++;
            return true;
        }

        public bool TryAddFlushCommand()
        {
            var newHead = head;
            newHead += MessageUtil.WriteRedisArrayHeader(1, messageBuffer, newHead);
            newHead += MessageUtil.WriteRedisBulkString("FLUSHALL", messageBuffer, newHead);
            if (newHead == head)
                return false;

            head = newHead;
            commandCount++;
            return true;
        }
    }
}