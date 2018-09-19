// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace SumStore
{
    /// <summary>
    /// Custom interface of FASTER for user-specified types
    /// See cs\src\core\Index\FASTER\IFASTER.cs for template
    /// </summary>
    public unsafe interface ICustomFasterKv
    {
        /* Thread-related operations */
        Guid StartSession();
        long ContinueSession(Guid guid);
        void StopSession();
        void Refresh();

        /* Store Interface */
        Status Read(AdId* key, Input* input, Output* output, Empty* context, long lsn);
        Status Upsert(AdId* key, NumClicks* value, Empty* context, long lsn);
        Status RMW(AdId* key, Input* input, Empty* context, long lsn);
        bool CompletePending(bool wait);

        /* Recovery */
        bool TakeFullCheckpoint(out Guid token);
        bool TakeIndexCheckpoint(out Guid token);
        bool TakeHybridLogCheckpoint(out Guid token);
        void Recover(Guid fullcheckpointToken);
        void Recover(Guid indexToken, Guid hybridLogToken);

        /* Statistics */
        long LogTailAddress { get; }
        void DumpDistribution();
    }
}

