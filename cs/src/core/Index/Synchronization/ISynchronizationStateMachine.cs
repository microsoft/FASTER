// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Collections.Generic;
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
        /// Returns the version that we expect this state machine to end up at when back to REST, or -1 if not yet known.
        /// </summary>
        /// <returns> The version that we expect this state machine to end up at when back to REST </returns>
        long ToVersion();

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
        void GlobalBeforeEnteringState<Key, Value, StoreFunctions, Allocator>(SystemState next,
            FasterKV<Key, Value, StoreFunctions, Allocator> faster)
            where StoreFunctions : IStoreFunctions<Key, Value>
            where Allocator : AllocatorBase<Key, Value, StoreFunctions>;

        /// <summary>
        /// This function is invoked immediately after the global state machine enters the given state.
        /// </summary>
        /// <param name="next"></param>
        /// <param name="faster"></param>
        void GlobalAfterEnteringState<Key, Value, StoreFunctions, Allocator>(SystemState next,
            FasterKV<Key, Value, StoreFunctions, Allocator> faster)
            where StoreFunctions : IStoreFunctions<Key, Value>
            where Allocator : AllocatorBase<Key, Value, StoreFunctions>;

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
        /// <param name="valueTasks"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        void OnThreadEnteringState<Key, Value, Input, Output, Context, FasterSession, StoreFunctions, Allocator>(SystemState current,
            SystemState prev,
            FasterKV<Key, Value, StoreFunctions, Allocator> faster,
            FasterKV<Key, Value, StoreFunctions, Allocator>.FasterExecutionContext<Input, Output, Context> ctx,
            FasterSession fasterSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where FasterSession: IFasterSession
            where StoreFunctions : IStoreFunctions<Key, Value>
            where Allocator : AllocatorBase<Key, Value, StoreFunctions>;
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
        void GlobalBeforeEnteringState<Key, Value, StoreFunctions, Allocator>(
            SystemState next,
            FasterKV<Key, Value, StoreFunctions, Allocator> faster)
            where StoreFunctions : IStoreFunctions<Key, Value>
            where Allocator : AllocatorBase<Key, Value, StoreFunctions>;

        /// <summary>
        /// This function is invoked immediately after the global state machine enters the given state.
        /// </summary>
        /// <param name="next"></param>
        /// <param name="faster"></param>
        void GlobalAfterEnteringState<Key, Value, StoreFunctions, Allocator>(
            SystemState next,
            FasterKV<Key, Value, StoreFunctions, Allocator> faster)
            where StoreFunctions : IStoreFunctions<Key, Value>
            where Allocator : AllocatorBase<Key, Value, StoreFunctions>;

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
        /// <param name="valueTasks"></param>
        /// <param name="token"></param>
        void OnThreadState<Key, Value, Input, Output, Context, FasterSession, StoreFunctions, Allocator>(
            SystemState current,
            SystemState prev,
            FasterKV<Key, Value, StoreFunctions, Allocator> faster,
            FasterKV<Key, Value, StoreFunctions, Allocator>.FasterExecutionContext<Input, Output, Context> ctx,
            FasterSession fasterSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where FasterSession: IFasterSession
            where StoreFunctions : IStoreFunctions<Key, Value>
            where Allocator : AllocatorBase<Key, Value, StoreFunctions>;
    }

    /// <summary>
    /// Abstract base class for ISynchronizationStateMachine that implements that state machine logic
    /// with ISynchronizationTasks
    /// </summary>
    internal abstract class SynchronizationStateMachineBase : ISynchronizationStateMachine
    {
        private readonly ISynchronizationTask[] tasks;
        private long toVersion = -1;
        

        /// <summary>
        /// Construct a new SynchronizationStateMachine with the given tasks. The order of tasks given is the
        /// order they are executed on each state machine.
        /// </summary>
        /// <param name="tasks">The ISynchronizationTasks to run on the state machine</param>
        protected SynchronizationStateMachineBase(params ISynchronizationTask[] tasks)
        {
            this.tasks = tasks;
        }

        /// <summary>
        /// Sets ToVersion for return. Defaults to -1 if not set
        /// </summary>
        /// <param name="v"> toVersion </param>
        protected void SetToVersion(long v) => toVersion = v;

        /// <inheritdoc />
        public long ToVersion() => toVersion;
        
        /// <inheritdoc />
        public abstract SystemState NextState(SystemState start);

        /// <inheritdoc />
        public void GlobalBeforeEnteringState<Key, Value, StoreFunctions, Allocator>(SystemState next,
            FasterKV<Key, Value, StoreFunctions, Allocator> faster)
            where StoreFunctions : IStoreFunctions<Key, Value>
            where Allocator : AllocatorBase<Key, Value, StoreFunctions>
        {
            foreach (var task in tasks)
                task.GlobalBeforeEnteringState(next, faster);
        }

        /// <inheritdoc />
        public void GlobalAfterEnteringState<Key, Value, StoreFunctions, Allocator>(SystemState next,
            FasterKV<Key, Value, StoreFunctions, Allocator> faster)
            where StoreFunctions : IStoreFunctions<Key, Value>
            where Allocator: AllocatorBase<Key, Value, StoreFunctions>
        {
            foreach (var task in tasks)
                task.GlobalAfterEnteringState(next, faster);
        }

        /// <inheritdoc />
        public void OnThreadEnteringState<Key, Value, Input, Output, Context, FasterSession, StoreFunctions, Allocator>(
            SystemState current,
            SystemState prev,
            FasterKV<Key, Value, StoreFunctions, Allocator> faster,
            FasterKV<Key, Value, StoreFunctions, Allocator>.FasterExecutionContext<Input, Output, Context> ctx,
            FasterSession fasterSession,
            List<ValueTask> valueTasks,
            CancellationToken token = default)
            where FasterSession: IFasterSession
            where StoreFunctions : IStoreFunctions<Key, Value>
            where Allocator : AllocatorBase<Key, Value, StoreFunctions>
        {
            foreach (var task in tasks)
            {
                task.OnThreadState(current, prev, faster, ctx, fasterSession, valueTasks, token);
            }
        }
    }
}