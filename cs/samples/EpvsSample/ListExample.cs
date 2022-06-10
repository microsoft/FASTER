using System;
using System.Threading;
using FASTER.core;

namespace EpvsSample
{
    public interface IResizableList
    {
        public int Count();

        public long Read(int index);

        public void Write(int index, long value);

        public int Push(long value);
    }


    public class SingleThreadedResizableList : IResizableList
    {
        private long[] list;
        private int count;

        public SingleThreadedResizableList()
        {
            list = new long[16];
            count = 0;
        }

        public int Count() => count;

        public long Read(int index)
        {
            if (index < 0 || index >= count) throw new IndexOutOfRangeException();
            return list[index];
        }

        public void Write(int index, long value)
        {
            if (index < 0 || index >= count) throw new IndexOutOfRangeException();
            list[index] = value;
        }

        public int Push(long value)
        {
            if (count == list.Length)
            {
                var newList = new long[2 * count];
                Array.Copy(list, newList, list.Length);
                list = newList;
            }

            list[count] = value;
            return count++;
        }
    }

    public class LatchedResizableList : IResizableList
    {
        private ReaderWriterLockSlim rwLatch;
        private long[] list;
        private int count;

        public LatchedResizableList()
        {
            rwLatch = new ReaderWriterLockSlim();
            list = new long[16];
            count = 0;
        }

        public int Count() => Math.Min(count, list.Length);

        public long Read(int index)
        {
            if (index < 0) throw new IndexOutOfRangeException();
            try
            {
                rwLatch.EnterReadLock();
                if (index < list.Length) return list[index];
                if (index < count) return default;
                throw new IndexOutOfRangeException();
            }
            finally
            {
                rwLatch.ExitReadLock();
            }
        }

        public void Write(int index, long value)
        {
            rwLatch.EnterReadLock();
            list[index] = value;
            rwLatch.ExitReadLock();
        }

        private void Resize()
        {
            try
            {
                rwLatch.EnterWriteLock();
                var newList = new long[2 * list.Length];
                Array.Copy(list, newList, list.Length);
                list = newList;
            }
            finally
            {
                rwLatch.ExitWriteLock();
            }
        }

        public int Push(long value)
        {
            var result = Interlocked.Increment(ref count) - 1;
            while (true)
            {
                if (result == list.Length)
                    Resize();
                try
                {
                    rwLatch.EnterReadLock();
                    if (result >= list.Length) continue;
                    list[result] = value;
                    return result;
                }
                finally
                {
                    rwLatch.ExitReadLock();
                }
            }
        }
    }

    public class SimpleVersionSchemeResizableList : IResizableList
    {
        private EpochProtectedVersionScheme epvs;
        private long[] list;
        private int count;

        public SimpleVersionSchemeResizableList()
        {
            epvs = new EpochProtectedVersionScheme(new LightEpoch());
            list = new long[16];
            count = 0;
        }

        public int Count() => Math.Min(count, list.Length);

        public long Read(int index)
        {
            if (index < 0) throw new IndexOutOfRangeException();
            try
            {
                epvs.Enter();
                if (index < list.Length) return list[index];
                if (index < count) return default;
                throw new IndexOutOfRangeException();
            }
            finally
            {
                epvs.Leave();
            }
        }

        public void Write(int index, long value)
        {
            try
            {
                epvs.Enter();
                if (index < 0 || index >= count) throw new IndexOutOfRangeException();
                list[index] = value;
            }
            finally
            {
                epvs.Leave();
            }
        }

        private void Resize()
        {
            var newList = new long[2 * list.Length];
            Array.Copy(list, newList, list.Length);
            list = newList;
        }

        public int Push(long value)
        {
            try
            {
                var v = epvs.Enter();
                var result = Interlocked.Increment(ref count) - 1;

                while (true)
                {
                    if (result < list.Length)
                    {
                        list[result] = value;
                        return result;
                    }

                    epvs.Leave();
                    if (result == list.Length)
                        epvs.AdvanceVersionWithCriticalSection((_, _) => Resize(), v.Version + 1);
                    Thread.Yield();
                    v = epvs.Enter();
                }
            }
            finally
            {
                epvs.Leave();
            }
        }
    }

    public class ListGrowthStateMachine : VersionSchemeStateMachine
    {
        public const byte COPYING = 1;
        private TwoPhaseResizableList obj;
        private volatile bool copyDone = false;

        public ListGrowthStateMachine(TwoPhaseResizableList obj, long toVersion) : base(toVersion)
        {
            this.obj = obj;
        }

        public override bool GetNextStep(VersionSchemeState currentState, out VersionSchemeState nextState)
        {
            switch (currentState.Phase)
            {
                case VersionSchemeState.REST:
                    nextState = VersionSchemeState.Make(COPYING, currentState.Version);
                    return true;
                case COPYING:
                    nextState = VersionSchemeState.Make(VersionSchemeState.REST, actualToVersion);
                    return copyDone;
                default:
                    throw new NotImplementedException();
            }
        }

        public override void OnEnteringState(VersionSchemeState fromState, VersionSchemeState toState)
        {
            switch (fromState.Phase)
            {
                case VersionSchemeState.REST:
                    obj.newList = new long[obj.list.Length * 2];
                    break;
                case COPYING:
                    obj.list = obj.newList;
                    break;
                default:
                    throw new NotImplementedException();
            }
        }

        public override void AfterEnteringState(VersionSchemeState state)
        {
            if (state.Phase == COPYING)
            {
                Array.Copy(obj.list, obj.newList, obj.list.Length);
                copyDone = true;
                obj.epvs.SignalStepAvailable();
            }
        }
    }

    public class TwoPhaseResizableList : IResizableList
    {
        internal EpochProtectedVersionScheme epvs;
        internal long[] list, newList;
        internal int count;

        public TwoPhaseResizableList()
        {
            epvs = new EpochProtectedVersionScheme(new LightEpoch());
            list = new long[16];
            newList = list;
            count = 0;
        }

        // TODO(Tianyu): How to ensure this is correct in the face of concurrent pushes?
        public int Count() => count;

        public long Read(int index)
        {
            if (index < 0) throw new IndexOutOfRangeException();
            try
            {
                var state = epvs.Enter();
                switch (state.Phase)
                {
                    case VersionSchemeState.REST:
                        if (index < list.Length) return list[index];
                        // element allocated but yet to be constructed
                        if (index < count) return default;
                        throw new IndexOutOfRangeException();
                    case ListGrowthStateMachine.COPYING:
                        if (index < list.Length) return list[index];
                        if (index < newList.Length) return newList[index];
                        if (index < count) return default;
                        throw new IndexOutOfRangeException();
                    default:
                        throw new NotImplementedException();
                }
            }
            finally
            {
                epvs.Leave();
            }
        }

        public void Write(int index, long value)
        {
            try
            {
                var state = epvs.Enter();
                // Write operation is not allowed during copy because we don't know about the copy progress
                while (state.Phase == ListGrowthStateMachine.COPYING)
                {
                    state = epvs.Refresh();
                    Thread.Yield();
                }

                if (index < 0 || index >= count) throw new IndexOutOfRangeException();
                list[index] = value;
            }
            finally
            {
                epvs.Leave();
            }
        }

        public int Push(long value)
        {
            try
            {
                var result = Interlocked.Increment(ref count) - 1;

                var state = epvs.Enter();

                // Write the entry into the correct underlying array
                while (true)
                {
                    if (state.Phase == VersionSchemeState.REST && result == list.Length)
                    {
                        epvs.Leave();
                        // Use explicit versioning to prevent multiple list growth resulting from same full list state
                        epvs.ExecuteStateMachine(new ListGrowthStateMachine(this, state.Version + 1));
                        state = epvs.Enter();
                    }

                    switch (state.Phase)
                    {
                        case VersionSchemeState.REST:
                            if (result >= list.Length)
                            {
                                epvs.Leave();
                                Thread.Yield();
                                state = epvs.Enter();
                                continue;
                            }

                            list[result] = value;
                            return result;
                        case ListGrowthStateMachine.COPYING:
                            // This was the copying phase of a previous state machine
                            if (result >= newList.Length)
                            {
                                epvs.Leave();
                                Thread.Yield();
                                state = epvs.Enter();
                                continue;
                            }

                            // Make sure to write update to old list if it belongs there in case copying erases new write
                            if (result < list.Length)
                                list[result] = value;
                            // Also write to new list in case copying was delayed
                            newList[result] = value;
                            return result;
                    }
                }
            }
            finally
            {
                epvs.Leave();
            }
        }
    }
}