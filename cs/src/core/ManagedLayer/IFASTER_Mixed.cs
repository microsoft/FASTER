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
    /// <summary>
    /// Interface for FASTER
    /// </summary>
    public unsafe interface IFasterKV_Mixed : IDisposable
    {
        /* Thread-related operations */
        
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        Guid StartSession();
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="guid"></param>
        /// <returns></returns>
        long ContinueSession(Guid guid);

        /// <summary>
        /// 
        /// </summary>
        void StopSession();

        /// <summary>
        /// 
        /// </summary>
        void Refresh();

        /* Store Interface */

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="context"></param>
        /// <param name="lsn"></param>
        /// <returns></returns>
        Status Read(MixedKeyWrapper* key, MixedInputWrapper* input, MixedOutputWrapper* output, MixedContextWrapper* context, long lsn);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="context"></param>
        /// <param name="lsn"></param>
        /// <returns></returns>
        Status Upsert(MixedKeyWrapper* key, MixedValueWrapper* value, MixedContextWrapper* context, long lsn);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <param name="lsn"></param>
        /// <returns></returns>
        Status RMW(MixedKeyWrapper* key, MixedInputWrapper* input, MixedContextWrapper* context, long lsn);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="wait"></param>
        /// <returns></returns>
        bool CompletePending(bool wait);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="untilAddress"></param>
        /// <returns></returns>
        bool ShiftBeginAddress(long untilAddress);

        /* Recovery */

        /// <summary>
        /// 
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        bool TakeFullCheckpoint(out Guid token);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        bool TakeIndexCheckpoint(out Guid token);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        bool TakeHybridLogCheckpoint(out Guid token);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fullcheckpointToken"></param>
        void Recover(Guid fullcheckpointToken);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="indexToken"></param>
        /// <param name="hybridLogToken"></param>
        void Recover(Guid indexToken, Guid hybridLogToken);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="wait"></param>
        /// <returns></returns>
        bool CompleteCheckpoint(bool wait);

        /* Statistics */

        /// <summary>
        /// 
        /// </summary>
        long LogTailAddress { get; }

        /// <summary>
        /// 
        /// </summary>
        long LogReadOnlyAddress { get; }

        /// <summary>
        /// 
        /// </summary>
        long EntryCount { get; }

        /// <summary>
        /// 
        /// </summary>
        void DumpDistribution();
    }
}
