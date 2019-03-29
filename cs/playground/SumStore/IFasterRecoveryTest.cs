// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace SumStore
{
    public interface IFasterRecoveryTest
    {
        void Populate();
        void Continue();
        void RecoverLatest();
        void Recover(Guid indexToken, Guid hybridLogToken);
    }
}
