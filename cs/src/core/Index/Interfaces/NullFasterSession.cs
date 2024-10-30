namespace FASTER.core
{
    struct NullFasterSession : IFasterSession
    {
        public static readonly NullFasterSession Instance = new();

        public readonly void CheckpointCompletionCallback(int sessionID, string sessionName, CommitPoint commitPoint)
        {
        }

        public readonly void UnsafeResumeThread()
        {
        }

        public readonly void UnsafeSuspendThread()
        {
        }
    }
}