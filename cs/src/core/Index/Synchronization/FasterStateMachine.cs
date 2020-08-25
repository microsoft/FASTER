using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public partial class FasterKV<Key, Value>
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

        internal SystemState SystemState => systemState;

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
            // Between state transition, temporarily block any concurrent execution thread 
            // from progressing to prevent perceived inconsistencies
            var intermediate = SystemState.MakeIntermediate(expectedState);
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
            return currentGlobalState.phase < Phase.REST
                ? SystemState.Make(Phase.REST, currentGlobalState.version - 1)
                : SystemState.Make(Phase.REST, currentGlobalState.version);
        }

        // Given the current thread state and global state, fast forward the thread state to the
        // current state machine cycle if needed
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private SystemState FastForwardToCurrentCycle(SystemState threadState, SystemState targetStartState)
        {
            if (threadState.version < targetStartState.version ||
                threadState.version == targetStartState.version && threadState.phase < targetStartState.phase)
            {
                return targetStartState;
            }

            return threadState;
        }

        /// <summary>
        /// Steps the thread's local state machine. Threads catch up to the current global state and performs
        /// necessary actions associated with the state as defined by the current state machine
        /// </summary>
        /// <param name="ctx">null if calling without a context (e.g. waiting on a checkpoint)</param>
        /// <param name="fasterSession">Faster session.</param>
        /// <param name="valueTasks">Return list of tasks that caller needs to await, to continue checkpointing</param>
        /// <param name="token">Cancellation token</param>
        /// <returns></returns>
        private void ThreadStateMachineStep<Input, Output, Context, FasterSession>(
            FasterExecutionContext<Input, Output, Context> ctx,
            FasterSession fasterSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where FasterSession : IFasterSession
        {

            #region Capture current (non-intermediate) system state
            var currentTask = currentSyncStateMachine;
            var targetState = SystemState.Copy(ref systemState);
            SystemState.RemoveIntermediate(ref targetState);

            while (currentSyncStateMachine != currentTask)
            {
                currentTask = currentSyncStateMachine;
                targetState = SystemState.Copy(ref systemState);
                SystemState.RemoveIntermediate(ref targetState);
            }
            #endregion

            var currentState = ctx == null ? targetState : SystemState.Make(ctx.phase, ctx.version);
            var targetStartState = StartOfCurrentCycle(targetState);

            #region Get returning thread to start of current cycle, issuing completion callbacks if needed
            if (ctx != null)
            {
                if (ctx.version < targetStartState.version)
                {
                    // Issue CPR callback for full session
                    if (ctx.serialNum != -1)
                    {
                        List<long> excludedSerialNos = new List<long>();
                        foreach (var v in ctx.ioPendingRequests.Values)
                        {
                            excludedSerialNos.Add(v.serialNum);
                        }
                        foreach (var v in ctx.retryRequests)
                        {
                            excludedSerialNos.Add(v.serialNum);
                        }

                        var commitPoint = new CommitPoint
                        {
                            UntilSerialNo = ctx.serialNum,
                            ExcludedSerialNos = excludedSerialNos
                        };

                        // Thread local action
                        fasterSession?.CheckpointCompletionCallback(ctx.guid, commitPoint);
                    }
                }
                if ((ctx.version == targetStartState.version) && (ctx.phase < Phase.REST))
                {
                    // Ensure atomic switch took place. Would not have happened
                    // for index-only checkpoints.
                    if (ctx.prevCtx.excludedSerialNos != null)
                    {
                        // Issue CPR callback on old version (prevCtx)
                        if (ctx.prevCtx.serialNum != -1)
                        {
                            var commitPoint = new CommitPoint
                            {
                                UntilSerialNo = ctx.prevCtx.serialNum,
                                ExcludedSerialNos = ctx.prevCtx.excludedSerialNos
                            };

                            // Thread local action
                            fasterSession?.CheckpointCompletionCallback(ctx.guid, commitPoint);
                            ctx.prevCtx.excludedSerialNos = null;
                        }
                    }
                }
            }
            #endregion 

            // No state machine associated with target, or target is in REST phase:
            // we can directly fast forward session to target state
            if (currentTask == null || targetState.phase == Phase.REST)
            {
                if (ctx != null)
                {
                    ctx.phase = targetState.phase;
                    ctx.version = targetState.version;
                }
                return;
            }

            #region Jump on and execute current state machine
            // We start at either the start point or our previous position in the state machine.
            // If we are calling from somewhere other than an execution thread (e.g. waiting on
            // a checkpoint to complete on a client app thread), we start at current system state
            var threadState = 
                ctx == null ? targetState :
                FastForwardToCurrentCycle(currentState, targetStartState);

            var previousState = threadState;
            do
            {
                Debug.Assert(
                    (threadState.version < targetState.version) ||
                    (threadState.version == targetState.version && threadState.phase <= targetState.phase)
                    );

                currentTask.OnThreadEnteringState(threadState, previousState, this, ctx, fasterSession, valueTasks, token);

                if (ctx != null)
                {
                    ctx.phase = threadState.phase;
                    ctx.version = threadState.version;
                }   

                previousState.word = threadState.word;
                threadState = currentTask.NextState(threadState);
                if (systemState.word != targetState.word)
                {
                    var tmp = SystemState.Copy(ref systemState);
                    if (currentSyncStateMachine == currentTask)
                    {
                        targetState = tmp;
                        SystemState.RemoveIntermediate(ref targetState);
                    }
                }
            } while (previousState.word != targetState.word);
            #endregion

            return;
        }
    }
}