using FASTER.core;
using System.Threading.Tasks;

namespace AsyncStress
{
    public interface IFasterWrapper<Key, Value>
    {
        long TailAddress { get; }
        int UpsertPendingCount { get; set; }
        bool UseOsReadBuffering { get; }

        void Dispose();
        ValueTask<(Status, Value)> Read(Key key);
        ValueTask<(Status, Value)> ReadAsync(Key key);
        ValueTask<(Status, Value)[]> ReadChunkAsync(Key[] chunk, int offset, int count);
        void Upsert(Key key, Value value);
        ValueTask UpsertAsync(Key key, Value value);
        ValueTask UpsertChunkAsync((Key, Value)[] chunk, int offset, int count);
    }
}