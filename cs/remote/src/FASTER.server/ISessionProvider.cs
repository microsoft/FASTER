using System;
using System.Collections.Generic;
using System.Net.Sockets;
using FASTER.common;
using FASTER.core;

namespace FASTER.server
{
    /// <summary>
    /// Interface to provides server-side session processing logic
    /// </summary>
    public interface ISessionProvider
    {
        /// <summary>
        /// Given messages of wire format type, returns a session that handles that wire format. If no provider is configured
        /// for the given wire format, an exception is thrown.
        /// </summary>
        /// <param name="wireFormat">Wire format</param>
        /// <param name="socket">Socket connection</param>
        /// <returns>Server session</returns>
        /// 

        /// <summary>
        /// Remove all subscriptions made by a particular session.
        /// </summary>
        /// <param name="session">Session whose subscriptions to be removed</param>

        IServerSession GetSession(WireFormat wireFormat, Socket socket, SubscribeKVBroker subscribeKVBroker);

        public unsafe bool CheckSubKeyMatch(ref byte* subKeyPtr, ref byte* keyPtr);

        public unsafe void ReadAndPublish(byte* keyPtr, List<(IServerSession, int, bool)> subSessions);
    }
}