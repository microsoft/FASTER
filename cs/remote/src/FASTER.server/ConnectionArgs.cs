// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Net.Sockets;
using FASTER.core;
using FASTER.common;
using System;

namespace FASTER.server
{
    class ConnectionArgs<Key, Value, Input, Output, Functions, ParameterSerializer>
            where Functions : IFunctions<Key, Value, Input, Output, long>
            where ParameterSerializer : IServerSerializer<Key, Value, Input, Output>
    {
        public Socket socket;
        public FasterKV<Key, Value> store;
        public Func<WireFormat, Functions> functionsGen;
        public ParameterSerializer serializer;
        public ServerSessionBase<Key, Value, Input, Output, Functions, ParameterSerializer> session;
        public MaxSizeSettings maxSizeSettings;
    }
}
