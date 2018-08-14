// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace FASTER.core
{
    public interface IManagedFAST<K, V, I, O, C>
    {
        /* Thread-related operations */
        Guid StartSession();
        long ContinueSession(Guid guid);
        void StopSession();
        void Refresh();


        /* Store Interface */

        Status Read(K key, I input, ref O output, C context, long lsn);
        Status RMW(K key, I input, C context, long lsn);
        Status Upsert(K key, V value, C context, long lsn);
        Status Delete(K key, C context, long lsn);
        bool CompletePending(bool wait);

        /* Statistics */
        long Size { get; }
        void DumpDistribution();
    }
}
