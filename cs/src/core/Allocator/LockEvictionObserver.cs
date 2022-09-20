// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.core
{
    /// <summary>
    /// Observer for page-lock evictions
    /// </summary>
    public class LockEvictionObserver<Key, Value> : IObserver<IFasterScanIterator<Key, Value>>
    {
        readonly FasterKV<Key, Value> store;

        /// <summary>
        /// Class to manage lock eviction transfers to LockTable
        /// </summary>
        /// <param name="store">FASTER store instance</param>
        public LockEvictionObserver(FasterKV<Key, Value> store) => this.store = store;

        /// <summary>
        /// Subscriber to pages as they are getting evicted from main memory
        /// </summary>
        /// <param name="iter"></param>
        public void OnNext(IFasterScanIterator<Key, Value> iter)
        {
            while (iter.GetNext(out RecordInfo info))
            {
                // If it is not Invalid, we must Seal it so there is no possibility it will be missed while we're in the process
                // of transferring it to the Lock Table. Use manualLocking as we want to transfer the locks, not drain them.
                if (!info.IsLocked)
                    continue;

                // Seal it so there is no possibility it will be missed while we're in the process of transferring it to the Lock Table.
                // Use manualLocking as we want to transfer the locks, not drain them.
                info.Seal(manualLocking: true);

                // Now get it into the lock table, so it is ready as soon as the record is removed.
                this.store.LockTable.TransferFromLogRecord(ref iter.GetKey(), info);
            }
        }

        /// <summary>
        /// OnCompleted
        /// </summary>
        public void OnCompleted() { }

        /// <summary>
        /// OnError
        /// </summary>
        /// <param name="error"></param>
        public void OnError(Exception error) { }
    }
}
