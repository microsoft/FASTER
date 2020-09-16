namespace FASTER.core
{
    struct NullFasterSession : IFasterSession
    {
        public static readonly NullFasterSession Instance = default;

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