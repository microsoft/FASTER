using System;

namespace FASTER.libdpr
{
    public interface ISimpleStateObject<TToken>
    {
        void Operate(ref Span<byte> request, out Span<byte> reply);

        TToken PerformCheckpoint(Action<TToken> onPersist);

        void RestoreCheckpoint(TToken token);
    }
}