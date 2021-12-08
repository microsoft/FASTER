// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.common
{
    /// <summary>
    /// Interface for Network Sender
    /// </summary>
    public interface INetworkSender : IDisposable
    {
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        MaxSizeSettings GetMaxSizeSettings { get; }        

        /// <summary>
        /// Allocate a new response object
        /// </summary>
        void GetResponseObject();

        /// <summary>
        /// Free response object
        /// </summary>
        void ReturnResponseObject();

        /// <summary>
        /// Get current response object head ptr;
        /// </summary>
        /// <returns></returns>
        unsafe byte* GetResponseObjectHead();

        /// <summary>
        /// Get current response object tail ptr;
        /// </summary>
        /// <returns></returns>
        unsafe byte* GetResponseObjectTail();

        /// <summary>
        /// Send payload stored at response object of from head ptr to ptr + size
        /// </summary>
        /// <param name="size"></param>
        void SendResponse(int size);

        /// <summary>
        /// Send payload stored at response object of from head ptr + offset to ptr + offset + size
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        void SendResponse(int offset, int size);

        /// <summary>
        /// Dispose, optionally waiting for ongoing outgoing calls to complete
        /// </summary>
        void Dispose(bool waitForSendCompletion);
    }
}
