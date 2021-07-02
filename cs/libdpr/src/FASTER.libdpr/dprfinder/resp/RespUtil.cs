using System;
using System.Collections.Generic;
using System.Linq;
using FASTER.core;

namespace FASTER.libdpr
{
    internal static class RespUtil
    {
        internal static unsafe int LongToDecimalString(long a, byte[] buf, int offset)
        {
            var digits = stackalloc byte[20];
            var numDigits = 0;
            do
            {
                digits[numDigits] = (byte) (a % 10 + '0');
                numDigits++;
                a /= 10;
            } while (a > 0);

            var head = offset;

            if (head + numDigits >= buf.Length) return 0;
            for (var i = numDigits - 1; i >= 0; i--)
                buf[head++] = digits[i];
            return head - offset;
        }

        internal static long LongFromDecimalString(byte[] buf, int start, int end)
        {
            var negative = false;
            if (buf[start] == '-')
            {
                negative = true;
                start++;
            }

            long result = 0;

            for (var i = start; i < end; i++)
            {
                result *= 10;
                result += buf[i] - '0';
            }

            return negative ? -result : result;
        }

        internal static int WriteRedisBulkString(string val, byte[] buf, int offset)
        {
            var head = offset;
            if (head + 1 >= buf.Length) return 0;
            buf[head++] = (byte) '$';

            var size = LongToDecimalString(val.Length, buf, head);
            if (size == 0) return 0;
            head += size;

            if (head + 4 + val.Length >= buf.Length) return 0;
            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';

            foreach (var t in val)
                buf[head++] = (byte) t;

            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';
            return head - offset;
        }

        internal static int WriteRedisBulkString(long val, byte[] buf, int offset)
        {
            var head = offset;
            if (head + 1 >= buf.Length) return 0;
            buf[head++] = (byte) '$';

            var size = LongToDecimalString(sizeof(long), buf, head);
            if (size == 0) return 0;
            head += size;

            if (head + 4 + sizeof(long) >= buf.Length) return 0;
            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';

            BitConverter.TryWriteBytes(new Span<byte>(buf, head, sizeof(long)), val);
            head += sizeof(long);

            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';
            return head - offset;
        }

        internal static unsafe int WriteRedisBulkString(WorkerVersion val, byte[] buf, int offset)
        {
            var head = offset;
            if (head + sizeof(byte) >= buf.Length) return 0;
            buf[head++] = (byte) '$';

            var size = LongToDecimalString(sizeof(WorkerVersion), buf, head);
            if (size == 0) return 0;
            head += size;

            if (head + 4 + sizeof(WorkerVersion) >= buf.Length) return 0;
            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';

            BitConverter.TryWriteBytes(new Span<byte>(buf, head, sizeof(long)), val.Worker.guid);
            head += sizeof(long);
            BitConverter.TryWriteBytes(new Span<byte>(buf, head, sizeof(long)), val.Version);
            head += sizeof(long);

            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';
            return head - offset;
        }

        internal static unsafe int WriteRedisBulkString(IEnumerable<WorkerVersion> val, byte[] buf, int offset)
        {
            var head = offset;
            if (head + sizeof(byte) >= buf.Length) return 0;
            buf[head++] = (byte) '$';

            // Find size of encoding up front
            var count = val.Count();
            var totalSize = sizeof(int) + count * sizeof(WorkerVersion);

            var size = LongToDecimalString(totalSize, buf, head);
            if (size == 0) return 0;
            head += size;

            if (head + 4 + totalSize >= buf.Length) return 0;
            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';

            BitConverter.TryWriteBytes(new Span<byte>(buf, head, sizeof(int)), count);
            head += sizeof(int);
            foreach (var wv in val)
            {
                BitConverter.TryWriteBytes(new Span<byte>(buf, head, sizeof(long)), wv.Worker.guid);
                head += sizeof(long);
                BitConverter.TryWriteBytes(new Span<byte>(buf, head, sizeof(long)), wv.Version);
                head += sizeof(long);
            }

            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';
            return head - offset;
        }

        internal static int WriteRedisArrayHeader(int numElems, byte[] buf, int offset)
        {
            var head = offset;
            if (head + 1 >= buf.Length) return 0;
            buf[head++] = (byte) '*';

            var size = LongToDecimalString(numElems, buf, head);
            if (size == 0) return 0;
            head += size;

            if (head + 2 >= buf.Length) return 0;
            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';
            return head - offset;
        }

        internal static int DictionarySerializedSize(IDictionary<Worker, long> dict)
        {
            return sizeof(int) + dict.Count * 2 * sizeof(long);
        }

        internal static int SerializeDictionary(IDictionary<Worker, long> dict, byte[] buf, int head)
        {
            if (head + DictionarySerializedSize(dict) > buf.Length) return 0;
            BitConverter.TryWriteBytes(new Span<byte>(buf, head, sizeof(int)), dict.Count);
            head += sizeof(int);
            foreach (var (worker, val) in dict)
            {
                BitConverter.TryWriteBytes(new Span<byte>(buf, head, sizeof(long)), worker.guid);
                head += sizeof(long);
                BitConverter.TryWriteBytes(new Span<byte>(buf, head, sizeof(long)), val);
                head += sizeof(long);
            }

            return head;
        }

        internal static int ReadDictionaryFromBytes(byte[] buf, int head, IDictionary<Worker, long> result)
        {
            var size = BitConverter.ToInt32(buf, head);
            head += sizeof(int);
            for (var i = 0; i < size; i++)
            {
                var workerId = BitConverter.ToInt64(buf, head);
                head += sizeof(long);
                var val = BitConverter.ToInt64(buf, head);
                head += sizeof(long);
                result.TryAdd(new Worker(workerId), val);
            }

            return head;
        }
        
    }
}