using System;

namespace FASTER.core
{
    /// <summary>
    /// Callback functions to FASTER
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public interface IFunctions<Key, Value, Input, Output, Context>
    {
        /// <summary>
        /// Read completion
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="ctx"></param>
        /// <param name="status"></param>
        void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status);

        /// <summary>
        /// Upsert completion
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="ctx"></param>
        void UpsertCompletionCallback(ref Key key, ref Value value, Context ctx);

        /// <summary>
        /// RMW completion
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="ctx"></param>
        /// <param name="status"></param>
        void RMWCompletionCallback(ref Key key, ref Input input, Context ctx, Status status);

        /// <summary>
        /// Delete completion
        /// </summary>
        /// <param name="key"></param>
        /// <param name="ctx"></param>
        void DeleteCompletionCallback(ref Key key, Context ctx);

        /// <summary>
        /// Checkpoint completion callback (called per client session)
        /// </summary>
        /// <param name="sessionId">Session ID reporting persistence</param>
        /// <param name="serialNum">Checkpoint offset (CPR point) for session</param>
        void CheckpointCompletionCallback(Guid sessionId, long serialNum);

        /// <summary>
        /// Initial update for RMW
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="value"></param>
        void InitialUpdater(ref Key key, ref Input input, ref Value value);

        /// <summary>
        /// Copy-update for RMW
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="oldValue"></param>
        /// <param name="newValue"></param>
        void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue);

        /// <summary>
        /// In-place update for RMW
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="value"></param>
        void InPlaceUpdater(ref Key key, ref Input input, ref Value value);

        /// <summary>
        /// Single reader
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="value"></param>
        /// <param name="dst"></param>
        void SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst);

        /// <summary>
        /// Conncurrent reader
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="value"></param>
        /// <param name="dst"></param>
        void ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst);

        /// <summary>
        /// Single writer
        /// </summary>
        /// <param name="key"></param>
        /// <param name="src"></param>
        /// <param name="dst"></param>
        void SingleWriter(ref Key key, ref Value src, ref Value dst);

        /// <summary>
        /// Concurrent writer
        /// </summary>
        /// <param name="key"></param>
        /// <param name="src"></param>
        /// <param name="dst"></param>
        void ConcurrentWriter(ref Key key, ref Value src, ref Value dst);
    }

    /// <summary>
    /// Callback functions to FASTER, should be used with variable length structs
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    public interface IVariableLengthFunctions<Key, Value, Input>
    {
        /// <summary>
        /// Concurrent writer
        /// </summary>
        /// <param name="key"></param>
        /// <param name="src"></param>
        /// <param name="dst"></param>
        /// <returns>
        /// <code>true</code> - if it is possible to perform in place edit
        /// <code>false</code> - if a new record has to be created
        /// </returns>
        bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst);

        /// <summary>
        /// In-place update for RMW
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="value"></param>
        /// <returns>
        /// <code>true</code> - if it is possible to perform in place edit
        /// <code>false</code> - if a new record has to be created
        /// </returns>
        bool InPlaceUpdater(ref Key key, ref Input input, ref Value value);
    }
}