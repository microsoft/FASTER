using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public partial class FasterKV<Key, Value, Input, Output, Context>
        where Key : new()
        where Value : new()
    {
        // The current system state, defined as the combination of a phase and a version number. This value
        // is observed by all sessions and a state machine communicates its progress to sessions through
        // this value
        internal SystemState systemState;
        // This flag ensures that only one state machine is active at a given time.
        private volatile int stateMachineActive = 0;
        // The current state machine in the system. The value could be stale and point to the previous state machine
        // if no state machine is active at this time.
        private ISynchronizationStateMachine currentSyncStateMachine;

        /// <summary>
        /// Attempt to start the given state machine in the system if no other state machine is active.
        /// </summary>
        /// <param name="stateMachine">The state machine to start</param>
        /// <returns>true if the state machine has started, false otherwise</returns>
        private bool StartStateMachine(ISynchronizationStateMachine stateMachine)
        {
            // return immediately if there is a state machine under way.
            if (Interlocked.CompareExchange(ref stateMachineActive, 1, 0) != 0) return false;

            currentSyncStateMachine = stateMachine;
            // No latch required because the taskMutex guards against other tasks starting, and only a new task
            // is allowed to change faster global state from REST
            GlobalStateMachineStep(systemState);
            return true;
        }
        
        // Atomic transition from expectedState -> nextState
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool MakeTransition(SystemState expectedState, SystemState nextState)
        {
            if (Interlocked.CompareExchange(ref systemState.word, nextState.word, expectedState.word) !=
                expectedState.word) return false;
            Debug.WriteLine("Moved to {0}, {1}", nextState.phase, nextState.version);
            return true;
        }

        /// <summary>
        /// Steps the global state machine. This will change the current global system state and perform some actions
        /// as prescribed by the current state machine. This function has no effect if the current state is not
        /// the given expected state.
        /// </summary>
        /// <param name="expectedState">expected current global state</param>
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

            // Mark the state machine done as we exit the state machine.
            if (nextState.phase == Phase.REST) stateMachineActive = 0;
        }


        // Given the current global state, return the starting point of the state machine cycle
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private SystemState StartOfCurrentCycle(SystemState currentGlobalState)
        {
            return currentGlobalState.phase <= Phase.REST
                ? SystemState.Make(Phase.REST, currentGlobalState.version - 1)
                : SystemState.Make(Phase.REST, currentGlobalState.version);
        }

        // Given the current thread state and global state, fast forward the thread state to the
        // current state machine cycle if needed
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

        // Return the pair of current state machine and global state, guaranteed to be captured atomicaly.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private (ISynchronizationStateMachine, SystemState) CaptureTaskAndTargetState()
        {
            while (true)
            {
                var task = currentSyncStateMachine;
                var targetState = SystemState.Copy(ref systemState);
                // We have to make sure that we are not looking at a state resulted from a different 
                // task. It's ok to be behind when the thread steps through the state machine, but not
                // ok if we are using the wrong task.
                if (targetState.phase != Phase.INTERMEDIATE && currentSyncStateMachine == task)
                    return ValueTuple.Create(task, targetState);
            }
        }

        /// <summary>
        /// Steps the thread's local state machine. Threads catch up to the current global state and performs
        /// necessary actions associated with the state as defined by the current state machine
        /// </summary>
        /// <param name="ctx">null if calling without a context (e.g. waiting on a checkpoint)</param>
        /// <param name="functions">Callback functions.</param>
        /// <param name="clientSession">null if calling without a session (e.g. waiting on a checkpoint)</param>
        /// <param name="async"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        private async ValueTask ThreadStateMachineStep<Functions>(FasterExecutionContext ctx,
            Functions functions,
            ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
            bool async = true,
            CancellationToken token = default)
            where Functions: IFunctions<Key, Value, Input, Output, Context>
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
            var previousState = threadState;
            do
            {
                await currentTask.OnThreadEnteringState(threadState, previousState, this, ctx, functions, clientSession, async,
                    token);
                if (ctx != null)
                {
                    ctx.phase = threadState.phase;
                    ctx.version = threadState.version;
                }

                previousState.word = threadState.word;
                threadState = currentTask.NextState(threadState);
            } while (previousState.word != targetState.word);
        }

        /// <summary>
        /// Steps the thread's local state machine. Threads catch up to the current global state and performs
        /// necessary actions associated with the state as defined by the current state machine
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        private async ValueTask ThreadStateMachineStep(CancellationToken token = default)
        {
            // Target state is the current (non-intermediate state) system state thread needs to catch up to
            var (currentTask, targetState) = CaptureTaskAndTargetState();

            // the current thread state is what the thread remembers, or simply what the current system
            // is if we are calling from somewhere other than an execution thread (e.g. waiting on
            // a checkpoint to complete on a client app thread)
            var threadState = targetState;

            // If the thread was in the middle of handling some older, unrelated task, fast-forward to the current task
            // as the old one is no longer relevant
            threadState = FastForwardToCurrentCycle(threadState, targetState);
            var previousState = threadState;
            do
            {
                await currentTask.OnThreadEnteringState(threadState, previousState, this, token);

                previousState.word = threadState.word;
                threadState = currentTask.NextState(threadState);
            } while (previousState.word != targetState.word);
        }
    }
}