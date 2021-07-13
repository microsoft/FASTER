namespace FASTER.core
{
    struct NullFasterSession : IFasterSession
    {
        public static readonly NullFasterSession Instance = new();

        public void CheckpointCompletionCallback(string guid, CommitPoint commitPoint)
        {
        }

        public void UnsafeResumeThread()
        {
        }

        public void UnsafeSuspendThread()
        {
        }
    }
}