using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace FASTER.libdpr
{
    public static class Utility
    {
#if NETSTANDARD2_1
        public static bool TryWriteBytes(Span<byte> destination, long value) => BitConverter.TryWriteBytes(destination, value);
        
        public static bool TryWriteBytes(Span<byte> destination, int value) => BitConverter.TryWriteBytes(destination, value);
        
        public static IEnumerable<T> Append<T>(this IEnumerable<T> src, T elem) => src.Append(elem);
        
#else
        
        // TODO(TIanyu): Performance issue and should be fixed
        public static IEnumerable<T> Append<T>(this IEnumerable<T> src, T elem)
        {
            var l = src.ToList();
            l.Add(elem);
            return l;
        }

        public static unsafe bool TryWriteBytes(Span<byte> destination, long value)
        {
            if (destination.Length < sizeof(long)) return false;
            fixed (byte* bp = destination)
            {
                Unsafe.Write(bp, value);
            }

            return true;
        }

        public static unsafe bool TryWriteBytes(Span<byte> destination, int value)
        {
            if (destination.Length < sizeof(int)) return false;
            fixed (byte* bp = destination)
            {
                Unsafe.Write(bp, value);
            }

            return true;
        }

        public static TValue GetValueOrDefault<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key,
            TValue defaultValue)
        {
            return dictionary.TryGetValue(key, out var result) ? result : defaultValue;
        }

        public static bool TryAdd<TKey, TValue>(this Dictionary<TKey, TValue> dictionary, TKey key, TValue value)
        {
            if (dictionary.ContainsKey(key)) return false;
            dictionary.Add(key, value);
            return true;
        }

#endif
    }
}