// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using FASTER.common;

namespace FASTER.server
{
    /// <summary>
    /// 
    /// </summary>
    public interface IFasterServer : IDisposable
    {
        /// <summary>
        /// Register session provider for specified wire format with the server
        /// </summary>
        /// <param name="wireFormat"></param>
        /// <param name="backendProvider"></param>
        public void Register(WireFormat wireFormat, ISessionProvider backendProvider);

        /// <summary>
        /// Unregister provider associated with specified wire format
        /// </summary>
        /// <param name="wireFormat"></param>
        /// <param name="provider"></param>
        public void Unregister(WireFormat wireFormat, out ISessionProvider provider);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public ConcurrentDictionary<WireFormat, ISessionProvider> GetSessionProviders();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="protocol"></param>
        /// <param name="provider"></param>
        /// <param name="networkSender"></param>
        /// <param name="session"></param>
        /// <returns></returns>
        public bool AddSession(WireFormat protocol, ref ISessionProvider provider, INetworkSender networkSender, out IServerSession session);

        /// <summary>
        /// Start server
        /// </summary>
        public void Start();
    }
}