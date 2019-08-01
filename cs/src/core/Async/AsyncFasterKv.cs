using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace FASTER.core.async
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Functions"></typeparam>
    public class AsyncFasterKV<Key, Value, Input, Output, Functions>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, TaskCompletionSource<Output>>
    {
        private readonly FasterKV<Key, Value, Input, Output, TaskCompletionSource<Output>, Functions> fht;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        /// <param name="functions"></param>
        /// <param name="logSettings"></param>
        /// <param name="checkpointSettings"></param>
        /// <param name="serializerSettings"></param>
        /// <param name="comparer"></param>
        /// <param name="variableLengthStructSettings"></param>
        public AsyncFasterKV(long size, Functions functions, LogSettings logSettings, CheckpointSettings checkpointSettings = null, SerializerSettings<Key, Value> serializerSettings = null, IFasterEqualityComparer<Key> comparer = null, VariableLengthStructSettings<Key, Value> variableLengthStructSettings = null)
        {
            fht = new FasterKV<Key, Value, Input, Output, TaskCompletionSource<Output>, Functions>(size, functions, logSettings, checkpointSettings, serializerSettings, comparer, variableLengthStructSettings);

        }

        /// <summary>
        /// Read
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="monotonicSerialNum"></param>
        /// <returns></returns>
        public async ValueTask<Output> Read(Key key, Input input, Output output, long monotonicSerialNum)
        {
            TaskCompletionSource<Output> tcs = null;
            var status = fht.Read(ref key, ref input, ref output, ref tcs, monotonicSerialNum);
            if (status != Status.PENDING)
                return output;
            return await tcs.Task;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="monotonicSerialNum"></param>
        /// <returns></returns>
        public async ValueTask Upsert(Key key, Value desiredValue, long monotonicSerialNum)
        {
            TaskCompletionSource<Output> tcs = null;
            var status = fht.Upsert(ref key, ref desiredValue, ref tcs, monotonicSerialNum);
            if (status != Status.PENDING)
                return;
            await tcs.Task;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="wait"></param>
        /// <returns></returns>
        public ValueTask<bool> InternalCompletePending(bool wait = false)
        {
            return new ValueTask<bool>(fht.CompletePending(wait));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="untilSN"></param>
        /// <returns></returns>
        public ValueTask<Guid> TakeCheckpoint(long untilSN = -1)
        {
            throw new Exception();
        }
    }
}
