using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public interface ISynchronizationStateMachine
    {
        SystemState NextState(SystemState start);

        void GlobalBeforeEnteringState<Key, Value, Input, Output, Context, Functions>(SystemState next,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster)
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>;

        void GlobalAfterEnteringState<Key, Value, Input, Output, Context, Functions>(SystemState next,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster)
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>;

        ValueTask OnThreadEnteringState<Key, Value, Input, Output, Context, Functions>(SystemState entering,
            SystemState prev,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster,
            FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext ctx,
            ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
            bool async = true,
            CancellationToken token = default)
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>;
    }


    public interface ISynchronizationTask
    {
        void GlobalBeforeEnteringState<Key, Value, Input, Output, Context, Functions>(
            SystemState next,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster)
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>;

        void GlobalAfterEnteringState<Key, Value, Input, Output, Context, Functions>(
            SystemState next,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster)
            where Key : new ()
            where Value : new ()
            where Functions : IFunctions<Key, Value, Input, Output, Context>;

        ValueTask OnThreadState<Key, Value, Input, Output, Context, Functions>(
            SystemState current,
            SystemState prev,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster,
            FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext ctx,
            ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
            bool async = true,
            CancellationToken token = default)
            where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>;
    }


    public abstract class SynchronizationStateMachineBase : ISynchronizationStateMachine
    {
        private ISynchronizationTask[] tasks;

        protected SynchronizationStateMachineBase(params ISynchronizationTask[] tasks)
        {
            this.tasks = tasks;
        }

        public abstract SystemState NextState(SystemState start);

        public void GlobalBeforeEnteringState<Key, Value, Input, Output, Context, Functions>(SystemState next,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster) where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            foreach (var task in tasks)
                task.GlobalBeforeEnteringState(next, faster);
        }

        public void GlobalAfterEnteringState<Key, Value, Input, Output, Context, Functions>(SystemState next,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster) where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            foreach (var task in tasks)
                task.GlobalAfterEnteringState(next, faster);
        }

        public async ValueTask OnThreadEnteringState<Key, Value, Input, Output, Context, Functions>(
            SystemState entering,
            SystemState prev,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster,
            FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext ctx,
            ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, bool async = true,
            CancellationToken token = default) where Key : new()
            where Value : new()
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            foreach (var task in tasks)
                await task.OnThreadState(entering, prev, faster, ctx, clientSession, async, token);
        }
    }
}