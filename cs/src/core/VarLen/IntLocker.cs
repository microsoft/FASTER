using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    internal unsafe struct IntLocker
    {
        public const int kLatchBitMask = 1 << 31;
        public const int kMarkBitMask = 1 << 30;
        public const int kHeaderMask = 3 << 30;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SpinLock(ref int value)
        {
            while (true)
            {
                int expected_word = value;
                if ((expected_word & kLatchBitMask) == 0)
                {
                    var found_word = Interlocked.CompareExchange(ref value, expected_word | kLatchBitMask, expected_word);
                    if (found_word == expected_word) return;
                }
                Thread.Yield();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Unlock(ref int value)
        {
            value = value & ~kLatchBitMask;
        }

        public static void Mark(ref int value)
        {
            value |= kMarkBitMask;
        }

        public static void Unmark(ref int value)
        {
            value &= ~kMarkBitMask;
        }

        public static bool IsMarked(ref int value)
        {
            return (value & kMarkBitMask) != 0;
        }
    }
}