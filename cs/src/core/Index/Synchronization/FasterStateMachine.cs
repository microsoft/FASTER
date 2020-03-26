using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public partial class FasterKV<Key, Value, Input, Output, Context, Functions> : FasterBase,
        IFasterKV<Key, Value, Input, Output, Context, Functions>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        private SpinLock taskMutex = new SpinLock();
        private ISynchronizationStateMachine currentSyncStateMachine;

        internal bool StartStateMachine(ISynchronizationStateMachine stateMachine)
        {
            bool acquired = false;
            taskMutex.TryEnter(ref acquired);
            if (!acquired) return false;
            currentSyncStateMachine = stateMachine;
            // No latch required because the taskMutex guards against other tasks starting, and only a new task
            // is allowed to change faster global state from REST
            GlobalStateMachineStep(_systemState);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool MakeTransition(SystemState expectedState, SystemState nextState)
        {
            if (Interlocked.CompareExchange(ref _systemState.word, nextState.word, expectedState.word) !=
                expectedState.word) return false;
            Debug.WriteLine("Moved to {0}, {1}", nextState.phase, nextState.version);
            return true;
        }

        internal void GlobalStateMachineStep(SystemState expectedState)
        {
            // Between state transition, temporarily block any concurrent execution thread from progressing to prevent
            // perceived inconsistencies
            Debug.Assert(expectedState.phase != Phase.INTERMEDIATE, "Cannot step from intermediate");
            var intermediate = SystemState.Make(Phase.INTERMEDIATE, expectedState.version);
            if (!MakeTransition(expectedState, intermediate)) return;

            var nextState = currentSyncStateMachine.NextState(expectedState);

            // Execute custom task logic
            currentSyncStateMachine.GlobalBeforeEnteringState(nextState, this);
            var success = MakeTransition(intermediate, nextState);
            // Guaranteed to succeed, because other threads will always block while the system is in intermediate.
            Debug.Assert(success);
            currentSyncStateMachine.GlobalAfterEnteringState(nextState, this);
        }


        private SystemState StartOfCurrentCycle(SystemState currentGlobalState)
        {
            return currentGlobalState.phase <= Phase.REST
                ? SystemState.Make(Phase.REST, currentGlobalState.version - 1)
                : SystemState.Make(Phase.REST, currentGlobalState.version);
        }

        private SystemState FastForwardToCurrentCycle(SystemState currentThreadState, SystemState currentGlobalState)
        {
            var startState = StartOfCurrentCycle(currentGlobalState);
            if (currentThreadState.version < startState.version ||
                currentThreadState.version == startState.version && currentThreadState.phase < startState.phase)
            {
                return startState;
            }

            return currentThreadState;
        }

        private (ISynchronizationStateMachine, SystemState) CaptureTaskAndTargetState()
        {
            while (true)
            {
                var task = currentSyncStateMachine;
                var targetState = SystemState.Copy(ref _systemState);
                // We have to make sure that we are not looking at a state resulted from a different 
                // task. It's ok to be behind when the thread steps through the state machine, but not
                // ok if we are using the wrong task.
                if (targetState.phase != Phase.INTERMEDIATE && currentSyncStateMachine == task)
                    return ValueTuple.Create(task, targetState);
            }
        }

        internal async ValueTask ThreadStateMachineStep(FasterExecutionContext ctx,
            ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
            bool async = true,
            CancellationToken token = default)
        {
            if (async)
                clientSession?.UnsafeResumeThread();

            // Target state is the current (non-intermediate state) system state thread needs to catch up to
            var (currentTask, targetState) = CaptureTaskAndTargetState();

            // the current thread state is what the thread remembers, or simply what the current system
            // is if we are calling from somewhere other than an execution thread (e.g. waiting on
            // a checkpoint to complete on a client app thread)
            var threadState = ctx == null ? targetState : SystemState.Make(ctx.phase, ctx.version);

            // If the thread was in the middle of handling some older, unrelated task, fast-forward to the current task
            // as the old one is no longer relevant
            threadState = FastForwardToCurrentCycle(threadState, targetState);
            // TODO(Tianyu): WTF is this previous state thing
            var previousState = threadState;
            do
            {
                await currentTask.OnThreadEnteringState(threadState, previousState, this, ctx, clientSession, async,
                    token);
                if (ctx != null)
                {
                    ctx.phase = threadState.phase;
                    ctx.version = threadState.version;
                }

                previousState.word = threadState.word;
                threadState = currentTask.NextState(threadState);
            } while (threadState.word != targetState.word);
        }
    }
}