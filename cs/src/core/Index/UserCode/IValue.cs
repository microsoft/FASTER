using System.IO;

namespace FASTER.core
{
    public interface IValue<Value>
    {
        int GetLength();
        void ShallowCopy(ref Value dst);
        void Free();

        void AcquireReadLock();
        void AcquireWriteLock();
        void ReleaseReadLock();
        void ReleaseWriteLock();

        bool HasObjectsToSerialize();
        void Serialize(Stream toStream);
        void Deserialize(Stream fromStream);
        ref Value MoveToContext(ref Value value);
    }
}