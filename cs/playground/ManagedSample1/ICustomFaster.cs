// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace ManagedSample1
{
    public unsafe interface ICustomFaster
    {
        /* Thread-related operations */
        Guid StartSession();
        long ContinueSession(Guid guid);
        void StopSession();
        void Refresh();

        /* Store Interface */
        Status Read(KeyStruct* key, InputStruct* input, OutputStruct* output, Empty* context, long lsn);
        Status Upsert(KeyStruct* key, ValueStruct* value, Empty* context, long lsn);
        Status RMW(KeyStruct* key, InputStruct* input, Empty* context, long lsn);
        Status Delete(KeyStruct* key, Empty* context, long lsn);
        bool CompletePending(bool wait);

        /* Statistics */
        long Size { get; }
        void DumpDistribution();
    }
}

