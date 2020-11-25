namespace FASTER.core
{
    /// <summary>
    /// Optional functions to be called during compaction.
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    public interface ICompactionFunctions<Key, Value>
    {
        /// <summary>
        /// Checks if record in the faster log is logically deleted.
        /// If the record was deleted via <see cref="ClientSession{Key, Value, Input, Output, Context, Functions}.Delete(ref Key, Context, long)"/>
        /// then this function is not called for such a record.
        /// </summary>
        /// <remarks>
        /// <para>
        /// One possible scenario is if FASTER is used to store reference counted records.
        /// Once the record count reaches zero it can be considered to be no longer relevant and 
        /// compaction can skip the record.
        /// </para>
        /// <para>
        /// Compaction might be implemented by scanning the log thus it is possible that multiple
        /// records with the same key and/or different value might be provided. Only the last record will be persisted.
        /// </para>
        /// <para>
        /// This method can be called concurrently with methods in <see cref="IFunctions{Key, Value, Input, Output, Context}"/>. It is responsibility
        /// of the implementer to correctly manage concurrency.
        /// </para>
        /// </remarks>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool IsDeleted(in Key key, in Value value);

        /// <summary>
        /// Copies a value from <paramref name="src"/> to <paramref name="dst"/>.
        /// It is possible that value at <paramref name="src"/> might be modified during copy operation thus to prevent torn writes
        /// this method is provided to allow the implementer to correctly handle concurrency.
        /// This method is counterpart to <see cref="IFunctions{Key, Value, Input, Output, Context}.SingleWriter(ref Key, ref Value, ref Value)"/>.
        /// </summary>
        /// <param name="src">Managed pointer to value at source</param>
        /// <param name="dst">Managed pointer to uninitialized value at destination</param>
        /// <param name="valueLength">[Can be null] Variable length struct functions</param>
        /// <returns></returns>
        void Copy(ref Value src, ref Value dst, IVariableLengthStruct<Value> valueLength)
#if NETSTANDARD2_1
            { }
#else
            ;
#endif

        /// <summary>
        /// Copies a value from <paramref name="src"/> to <paramref name="dst"/>.
        /// It is possible that value at <paramref name="src"/> might be modified during copy operation thus to prevent torn writes
        /// this method is provided to allow the implementer to correctly handle concurrency.
        /// This method is counterpart to <see cref="IFunctions{Key, Value, Input, Output, Context}.ConcurrentWriter(ref Key, ref Value, ref Value)"/>.
        /// </summary>
        /// <param name="src">Managed pointer to value at source</param>
        /// <param name="dst">Managed pointer to existing value at destination</param>
        /// <param name="valueLength">[Can be null] Variable length struct functions</param>
        /// <returns>
        /// True - if <paramref name="src"/> can be safely copied to <paramref name="dst"/> (see <see cref="IFunctions{Key, Value, Input, Output, Context}.ConcurrentWriter(ref Key, ref Value, ref Value)"/>).
        /// False - if a new record needs to be allocated. In this case <see cref="ICompactionFunctions{Key, Value}.Copy(ref Value, ref Value, IVariableLengthStruct{Value})"/> will be called with 
        /// managed pointer to new record.
        /// </returns>
        bool CopyInPlace(ref Value src, ref Value dst, IVariableLengthStruct<Value> valueLength)
#if NETSTANDARD2_1
            => true
#endif
            ;
    }
}