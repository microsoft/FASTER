// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    // TODO: How are the sessions disposed?
    class SessionManager<TKey, TValue, TInput, TOutput, TFunctions>
        where TKey : struct
        where TValue : struct
        where TFunctions : IFunctions<TKey, TValue, TInput, TOutput, PSFContext>, new()
    {
        private readonly ConcurrentStack<ClientSession<TKey, TValue, TInput, TOutput, PSFContext, TFunctions>> freeSessions
            = new ConcurrentStack<ClientSession<TKey, TValue, TInput, TOutput, PSFContext, TFunctions>>();
        private readonly ConcurrentBag<ClientSession<TKey, TValue, TInput, TOutput, PSFContext, TFunctions>> allSessions
            = new ConcurrentBag<ClientSession<TKey, TValue, TInput, TOutput, PSFContext, TFunctions>>();

        internal FasterKV<TKey, TValue> fht;
        private readonly bool threadAffinitized;

        internal SessionManager(FasterKV<TKey, TValue> fht, bool threadAff)
        {
            this.fht = fht;
            this.threadAffinitized = threadAff;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ClientSession<TKey, TValue, TInput, TOutput, PSFContext, TFunctions> GetSession()
        {
            // Sessions are used only on post-RegisterPSF actions (Upsert, RMW, Query).
            if (this.freeSessions.TryPop(out var session))
                return session;
            session = this.fht.NewSession<TInput, TOutput, PSFContext, TFunctions>(new TFunctions(), threadAffinitized: this.threadAffinitized);
            this.allSessions.Add(session);
            return session;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ReleaseSession(ClientSession<TKey, TValue, TInput, TOutput, PSFContext, TFunctions> session)
        {
            // TODO: Cap on number of saved sessions?
            this.freeSessions.Push(session);
        }
    }
}
