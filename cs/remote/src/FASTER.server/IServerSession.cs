// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

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
        /// <param name="reqBuffer"></param>
        /// <param name="bytesRead"></param>
        /// <returns></returns>
        unsafe int TryConsumeMessages(byte* reqBuffer, int bytesRead);
    }
}
