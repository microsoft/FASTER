using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// Exclusive lock + marking using 2 MSB bits of int
    /// </summary>
    internal struct IntExclusiveLocker
    {
        const int kLatchBitMask = 1 << 31;
        const int kMarkBitMask = 1 << 30;
        public const int kHeaderMask = 3 << 30;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SpinLock(ref int value)
        {
            // Note: Any improvements here should be done in RecordInfo.SpinLock() as well.
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
            value &= ~kLatchBitMask;
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