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
        where Functions : IFunctions<Key, Value, Input, Output, Empty>
    {
        private readonly FasterKV<Key, Value, Input, Output, Empty, Functions> fht;

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
            fht = new FasterKV<Key, Value, Input, Output, Empty, Functions>(size, functions, logSettings, checkpointSettings, serializerSettings, comparer, variableLengthStructSettings);

        }

        /// <summary>
        /// Read
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="monotonicSerialNum"></param>
        /// <returns></returns>
        public async Task<Output> Read(Key key, Input input, Output output, long monotonicSerialNum)
        {
            var status = fht.Read(ref key, ref input, ref output, Empty.Default, monotonicSerialNum);
            if (status != Status.PENDING)
                return output;
            var tcs = new TaskCompletionSource<Output>();
            
            return await tcs.Task;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="monotonicSerialNum"></param>
        /// <returns></returns>
        public async Task Upsert(Key key, Value desiredValue, long monotonicSerialNum)
        {
            var status = fht.Upsert(ref key, ref desiredValue, Empty.Default, monotonicSerialNum);
            if (status != Status.PENDING)
                return;
            var tcs = new TaskCompletionSource<Output>();
            await tcs.Task;
        }


        public async Task<Guid> TakeCheckpoint(long untilSN = -1)
        {
            throw new Exception();
        }
    }
}
