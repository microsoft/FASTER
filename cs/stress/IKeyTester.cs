// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FASTER.stress
{
    internal interface IKeyTester : IDisposable
    {
        long GetAverageRecordSize();

        void Populate(int hashTableCacheLines, LogSettings logSettings, CheckpointSettings checkpointSettings);

        void Test();
        Task TestAsync();

        IValueTester ValueTester { get; }
    }
}
