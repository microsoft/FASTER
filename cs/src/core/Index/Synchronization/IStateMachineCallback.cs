namespace FASTER.core
{
    public interface IStateMachineCallback
    {
        void BeforeEnteringState<Key, Value>(SystemState next, FasterKV<Key, Value> faster);
    }
}