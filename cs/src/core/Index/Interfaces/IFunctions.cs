namespace FASTER.core
{
    public interface IFunctions<Key, Value, Input, Output, Context>
    {
        void ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst);
        void ConcurrentWriter(ref Key key, ref Value src, ref Value dst);
        void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue);
        void InitialUpdater(ref Key key, ref Input input, ref Value value);
        void InPlaceUpdater(ref Key key, ref Input input, ref Value value);
        void PersistenceCallback(long thread_id, long serial_num);
        void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, ref Context ctx, Status status);
        void RMWCompletionCallback(ref Key key, ref Input input, ref Context ctx, Status status);
        void SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst);
        void SingleWriter(ref Key key, ref Value src, ref Value dst);
        void UpsertCompletionCallback(ref Key key, ref Value value, ref Context ctx);
    }
}