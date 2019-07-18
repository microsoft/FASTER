// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    public interface ICheckpointDeviceManager
    {
        /// <summary>
        /// Index-related devices
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        IDevice GetIndexDevice(Guid token);
        IDevice GetOverflowBucketDevice(Guid token);
        IDevice GetIndexCheckpointInfoDevice(Guid token);
        IDevice GetIndexCheckpointCompletedDevice(Guid token);

        /// <summary>
        /// Log-related devices
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        IDevice GetSnapshotLogDevice(Guid token);
        IDevice GetSnapshotObjectLogDevice(Guid token);
        IDevice GetLogCheckpointInfoDevice(Guid token);
        IDevice GetLogCheckpointCompletedDevice(Guid token);

        /// <summary>
        /// Recovery helpers
        /// </summary>
        /// <param name="indexToken"></param>
        /// <param name="logToken"></param>
        /// <returns></returns>
        bool GetLatestValidCheckpoint(out Guid indexToken, out Guid logToken);
    }
}