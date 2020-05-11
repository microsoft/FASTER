// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Wrapper for the non-public OperationStatus
    /// </summary>
    public struct PSFOperationStatus
    {
        internal OperationStatus Status;

        internal PSFOperationStatus(OperationStatus opStatus) => this.Status = opStatus;
    }
}
