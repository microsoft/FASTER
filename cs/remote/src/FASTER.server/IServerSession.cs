// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.common;
using FASTER.core;
using System;

namespace FASTER.server
{
    /// <summary>
    /// Interface for server session provider
    /// </summary>
    public interface IServerSession : IDisposable
    {
        /// <summary>
        /// Consume the message incoming on the wire
        /// </summary>
        /// <param name="buf">Byte buffer</param>
        /// <returns>How many bytes were consumed</returns>
        int TryConsumeMessages(byte[] buf);

        /// <summary>
        /// Hook for caller to indicate how many additional bytes were read
        /// </summary>
        /// <param name="bytesRead"></param>
        void AddBytesRead(int bytesRead);

        unsafe void Publish(int sid, Status status, byte* outputPtr, int lengthOutput, byte* keyPtr, bool prefix);
    }

}
