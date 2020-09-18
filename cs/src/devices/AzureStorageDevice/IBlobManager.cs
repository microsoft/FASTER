// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.Azure.Storage.Blob;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.devices
{
    /// <summary>
    /// Manager for blobs, can be shared across devices.
    /// </summary>
    public interface IBlobManager
    {
        /// <summary>
        /// Get blob request options
        /// </summary>
        /// <returns></returns>
        BlobRequestOptions GetBlobRequestOptions();

        /// <summary>
        /// Cancellation token for blob operations
        /// </summary>
        CancellationToken CancellationToken { get; }

        /// <summary>
        /// Error handler for blob operations
        /// </summary>
        /// <param name="where"></param>
        /// <param name="message"></param>
        /// <param name="blobName"></param>
        /// <param name="e"></param>
        /// <param name="isFatal"></param>
        void HandleBlobError(string where, string message, string blobName, Exception e, bool isFatal);

        /// <summary>
        /// Confirm lease ownership
        /// </summary>
        /// <returns></returns>
        ValueTask ConfirmLeaseAsync();
    }
}