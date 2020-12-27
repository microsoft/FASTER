using System;

namespace FASTER.libdpr
{
    public interface ISimpleStateObject<TToken>
    {
        bool ProcessBatch(ReadOnlySpan<byte> request, out Span<byte> reply);

        TToken PerformCheckpoint(Action<TToken> onPersist);

        void RestoreCheckpoint(TToken token);
    }
}