// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.core
{
    internal class PageEvictionDisposeObserver<Key, Value, StoreFunctions> : IObserver<IFasterScanIterator<Key, Value>>
        where StoreFunctions : IStoreFunctions<Key, Value>
    {
        private readonly StoreFunctions storeFunctions;

        /// <summary>
        /// Class to call <see cref="IRecordDisposer{Key, Value}.DisposeRecord(ref Key, ref Value, DisposeReason)"/> on page evictions.
        /// </summary>
        /// <param name="storeFunctions">FASTER store functions</param>
        public PageEvictionDisposeObserver(StoreFunctions storeFunctions) => this.storeFunctions = storeFunctions;

        /// <summary>
        /// Subscriber to pages as they are getting evicted from main memory
        /// </summary>
        /// <param name="iter"></param>
        public void OnNext(IFasterScanIterator<Key, Value> iter)
        {
            while (iter.GetNext(out RecordInfo info, out Key key, out Value value))
                storeFunctions.DisposeRecord(ref key, ref value, DisposeReason.PageEviction);
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
