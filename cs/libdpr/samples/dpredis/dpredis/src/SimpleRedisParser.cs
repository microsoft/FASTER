using System;
using System.Diagnostics;

namespace dpredis
{
    public enum RedisMessageType
    {
        // TODO(Tianyu): Add more
        SIMPLE_STRING,
        ERROR,
        BULK_STRING
    }

    public struct SimpleRedisParser
    {
        public RedisMessageType currentMessageType;
        public int currentMessageStart;
        public int subMessageCount;

        public bool ProcessChar(int readHead, byte[] buf)
        {
            switch ((char) buf[readHead])
            {
                case '+':
                    if (currentMessageStart != -1) return false;
                    currentMessageStart = readHead;
                    currentMessageType = RedisMessageType.SIMPLE_STRING;
                    subMessageCount = 1;
                    return false;
                case '-':
                    // Special case for null bulk string
                    if (currentMessageType == RedisMessageType.BULK_STRING && readHead == currentMessageStart + 1)
                        subMessageCount = 1;
                    if (currentMessageStart != -1) return false;
                    currentMessageStart = readHead;
                    currentMessageType = RedisMessageType.ERROR;
                    subMessageCount = 1;
                    return false;
                case '$':
                    if (currentMessageStart != -1) return false;
                    Debug.Assert(currentMessageStart == -1);
                    currentMessageStart = readHead;
                    currentMessageType = RedisMessageType.BULK_STRING;
                    subMessageCount = 2;
                    return false;
                case ':':
                case '*':
                    if (currentMessageStart != -1) return false;
                    throw new NotImplementedException();
                // TODO(Tianyu): Wouldn't technically work if \r\n in value, but I am not planning to put any so who cares
                case '\n':
                    if (buf[readHead - 1] != '\r') return false;
                    Debug.Assert(currentMessageStart != -1);
                    // Special case for null string
                    return --subMessageCount == 0;
                default:
                    // Nothing to do
                    return false;
            }
        }
    }
}