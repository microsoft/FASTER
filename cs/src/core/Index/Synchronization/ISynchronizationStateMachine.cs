using System;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// A state machine defines a serious of actions that changes the system, which requires all sessions to
    /// synchronize and agree on certain time points. A full run of the state machine is defined as a cycle
    /// starting from REST and ending in REST, and only one state machine can be active at a given time.
    /// </summary>
    internal interface ISynchronizationStateMachine
    {
        /// <summary>
        /// This function models the transition function of a state machine.
        /// </summary>
        /// <param name="start">The current state of the state machine</param>
        /// <returns> the next state in this state machine </returns>
        SystemState NextState(SystemState start);

        /// <summary>
        /// This function is invoked immediately before the global state machine enters the given state.
        /// </summary>
        /// <param name="next"></param>
        /// <param name="faster"></param>
        /// <typeparam name="Key"></typeparam>
        /// <typeparam name="Value"></typeparam>
        void GlobalBeforeEnteringState<Key, Value>(SystemState next,
            FasterKV<Key, Value> faster)
            where Key : new()
            where Value : new();

        /// <summary>
        /// This function is invoked immediately after the global state machine enters the given state.
        /// </summary>
        /// <param name="next"></param>
        /// <param name="faster"></param>
        /// <typeparam name="Key"></typeparam>
        /// <typeparam name="Value"></typeparam>
        void GlobalAfterEnteringState<Key, Value>(SystemState next,
            FasterKV<Key, Value> faster)
            where Key : new()
            where Value : new();

        /// <summary>
        /// This function is invoked for every thread when they refresh and observe a given state.
        ///
        /// Note that the function is not allowed to await when async is set to false.
        /// </summary>
        /// <param name="current"></param>
        /// <param name="prev"></param>
        /// <param name="faster"></param>
        /// <param name="ctx"></param>
        /// <param name="fasterSession"></param>
        /// <param name="async"></param>
        /// <param name="token"></param>
        /// <typeparam name="Key"></typeparam>
        /// <typeparam name="Value"></typeparam>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        /// <typeparam name="FasterSession"></typeparam>
        /// <returns></returns>
        ValueTask OnThreadEnteringState<Key, Value, Input, Output, Context, FasterSession>(SystemState current,
            SystemState prev,
            FasterKV<Key, Value> faster,
            FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx,
            FasterSession fasterSession,
            bool async = true,
            CancellationToken token = default)
            where Key : new()
            where Value : new()
            where FasterSession: IFasterSession;
    }

    /// <summary>
    /// An ISynchronizationTask specifies logic to be run on a state machine, but does not specify a transition
    /// function. It is therefore possible to write common logic in an ISynchronizationTask and reuse it across
    /// multiple state machines, or to choose the task at runtime and achieve polymorphism in the behavior
    /// of a concrete state machine class.
    /// </summary>
    internal interface ISynchronizationTask
    {
        /// <summary>
        /// This function is invoked immediately before the global state machine enters the given state.
        /// </summary>
        /// <param name="next"></param>
        /// <param name="faster"></param>
        /// <typeparam name="Key"></typeparam>
        /// <typeparam name="Value"></typeparam>
        void GlobalBeforeEnteringState<Key, Value>(
            SystemState next,
            FasterKV<Key, Value> faster)
            where Key : new()
            where Value : new();

        /// <summary>
        /// This function is invoked immediately after the global state machine enters the given state.
        /// </summary>
        /// <param name="next"></param>
        /// <param name="faster"></param>
        /// <typeparam name="Key"></typeparam>
        /// <typeparam name="Value"></typeparam>
        void GlobalAfterEnteringState<Key, Value>(
            SystemState next,
            FasterKV<Key, Value> faster)
            where Key : new()
            where Value : new();

        /// <summary>
        /// This function is invoked for every thread when they refresh and observe a given state.
        ///
        /// Note that the function is not allowed to await when async is set to false.
        /// </summary>
        /// <param name="current"></param>
        /// <param name="prev"></param>
        /// <param name="faster"></param>
        /// <param name="ctx"></param>
        /// <param name="fasterSession"></param>
        /// <param name="async"></param>
        /// <param name="token"></param>
        /// <typeparam name="Key"></typeparam>
        /// <typeparam name="Value"></typeparam>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        /// <typeparam name="FasterSession"></typeparam>
        /// <returns></returns>
        ValueTask OnThreadState<Key, Value, Input, Output, Context, FasterSession>(
            SystemState current,
            SystemState prev,
            FasterKV<Key, Value> faster,
            FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx,
            FasterSession fasterSession,
            bool async = true,
            CancellationToken token = default)
            where Key : new()
            where Value : new()
            where FasterSession: IFasterSession;
    }

    /// <summary>
    /// Abstract base class for ISynchronizationStateMachine that implements that state machine logic
    /// with ISynchronizationTasks
    /// </summary>
    internal abstract class SynchronizationStateMachineBase : ISynchronizationStateMachine
    {
        private readonly ISynchronizationTask[] tasks;

        /// <summary>
        /// Construct a new SynchronizationStateMachine with the given tasks. The order of tasks given is the
        /// order they are executed on each state machine.
        /// </summary>
        /// <param name="tasks">The ISynchronizationTasks to run on the state machine</param>
        protected SynchronizationStateMachineBase(params ISynchronizationTask[] tasks)
        {
            this.tasks = tasks;
        }

        /// <inheritdoc />
        public abstract SystemState NextState(SystemState start);

        /// <inheritdoc />
        public void GlobalBeforeEnteringState<Key, Value>(SystemState next,
            FasterKV<Key, Value> faster) where Key : new()
            where Value : new()
        {
            foreach (var task in tasks)
                task.GlobalBeforeEnteringState(next, faster);
        }

        /// <inheritdoc />
        public void GlobalAfterEnteringState<Key, Value>(SystemState next,
            FasterKV<Key, Value> faster) where Key : new()
            where Value : new()
        {
            foreach (var task in tasks)
                task.GlobalAfterEnteringState(next, faster);
        }

        /// <inheritdoc />
        public async ValueTask OnThreadEnteringState<Key, Value, Input, Output, Context, FasterSession>(
            SystemState current,
            SystemState prev,
            FasterKV<Key, Value> faster,
            FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx,
            FasterSession fasterSession,
            bool async = true,
            CancellationToken token = default) where Key : new()
            where Value : new()
            where FasterSession: IFasterSession
        {
            foreach (var task in tasks)
            {
                if (async)
                    await task.OnThreadState(current, prev, faster, ctx, fasterSession, async, token);
                else
                {
                    var t = task.OnThreadState(current, prev, faster, ctx, fasterSession, async, token);
                    Debug.Assert(t.IsCompleted);
                }
            }
        }
    }
}