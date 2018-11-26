namespace FASTER.core
{
    public interface IMoveToContext<T>
    {
        ref T MoveToContext(ref T input);
    }
}