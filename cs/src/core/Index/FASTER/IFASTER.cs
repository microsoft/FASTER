// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    public unsafe interface IFASTER
    {
        /* Thread-related operations */
        Guid StartSession();
        long ContinueSession(Guid guid);
        void StopSession();
        void Refresh();
        bool TakeFullCheckpoint(out Guid token);
        bool TakeIndexCheckpoint(out Guid token);
        bool TakeHybridLogCheckpoint(out Guid token);
        void Recover(Guid fullcheckpointToken);
        void Recover(Guid indexToken, Guid hybridLogToken);

        /* Store Interface */
        Status Read(Key* key, Input* input, Output* output, Context* context, long lsn);
        Status Upsert(Key* key, Value* value, Context* context, long lsn);
        Status RMW(Key* key, Input* input, Context* context, long lsn);
        Status Delete(Key* key, Context* context, long lsn);
        bool CompletePending(bool wait);

        /* Statistics */
        long Size { get; }
        void DumpDistribution();
    }
}
