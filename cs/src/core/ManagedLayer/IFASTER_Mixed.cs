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
    public unsafe interface IFASTER_Mixed
    {
        /* Thread-related operations */
        Guid StartSession();
        long ContinueSession(Guid guid);
        void StopSession();
        void Refresh();

        /* Store Interface */
        Status Read(MixedKeyWrapper* key, MixedInputWrapper* input, MixedOutputWrapper* output, MixedContextWrapper* context, long lsn);
        Status Upsert(MixedKeyWrapper* key, MixedValueWrapper* value, MixedContextWrapper* context, long lsn);
        Status RMW(MixedKeyWrapper* key, MixedInputWrapper* input, MixedContextWrapper* context, long lsn);
        Status Delete(MixedKeyWrapper* key, MixedContextWrapper* context, long lsn);
        bool CompletePending(bool wait);

        /* Statistics */
        long Size { get; }
        void DumpDistribution();
    }
}
