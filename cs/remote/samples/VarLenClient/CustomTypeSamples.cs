// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using FASTER.client;
using FASTER.common;

namespace VarLenClient
{

    /// <summary>
    /// We demonstrate a client that talks to a VarLen server using a predefined type (VarLenType) 
    /// that conforms to the expected data format of [length][data]. The size of the type is
    /// pre-defined to be 12 bytes in VarLenType.
    /// </summary>
    class CustomTypeSamples
    {
        public void Run(string ip, int port)
        {
            using var client = new FasterKVClient<CustomType, CustomType>(ip, port);

            // Create a session to FasterKV server
            // Sessions are mono-threaded, similar to normal FasterKV sessions
            var session = client.NewSession(new CustomTypeFunctions(), WireFormat.DefaultVarLenKV);
            var subSession = client.NewSession(new CustomTypeFunctions(), WireFormat.DefaultVarLenKV);

            // Samples using sync client API
            SyncVarLenSamples(session);

            // Samples using sync client API
            SyncVarLenSubscriptionKVSamples(session, subSession);

            // Samples using sync client API
            SyncVarLenSubscriptionSamples(session, subSession);

            // Samples using async client API
            AsyncVarLenSamples(session).Wait();
        }

        void SyncVarLenSamples(ClientSession<CustomType, CustomType, CustomType, CustomType, byte, CustomTypeFunctions, FixedLenSerializer<CustomType, CustomType, CustomType, CustomType>> session)
        {
            for (int i = 0; i < 100; i++)
                session.Upsert(new CustomType(i), new CustomType(i + 10000));

            // Flushes partially filled batches, does not wait for responses
            session.Flush();

            session.Read(new CustomType(23));
            session.CompletePending(true);

            for (int i = 100; i < 200; i++)
                session.Upsert(new CustomType(i), new CustomType(i + 10000));

            session.Flush();

            session.CompletePending(true);
        }

        void SyncVarLenSubscriptionKVSamples(ClientSession<CustomType, CustomType, CustomType, CustomType, byte, CustomTypeFunctions, FixedLenSerializer<CustomType, CustomType, CustomType, CustomType>> session,
                                            ClientSession<CustomType, CustomType, CustomType, CustomType, byte, CustomTypeFunctions, FixedLenSerializer<CustomType, CustomType, CustomType, CustomType>> session2)
        {
            session2.SubscribeKV(new CustomType(23));
            session2.CompletePending(true);

            session2.SubscribeKV(new CustomType(24));
            session2.CompletePending(true);

            session2.PSubscribeKV(new CustomType(25));
            session2.CompletePending(true);

            session.Upsert(new CustomType(23), new CustomType(2300));
            session.CompletePending(true);

            session.Upsert(new CustomType(24), new CustomType(2400));
            session.CompletePending(true);

            session.Upsert(new CustomType(25), new CustomType(2500));
            session.CompletePending(true);

            System.Threading.Thread.Sleep(1000);
        }

        void SyncVarLenSubscriptionSamples(ClientSession<CustomType, CustomType, CustomType, CustomType, byte, CustomTypeFunctions, FixedLenSerializer<CustomType, CustomType, CustomType, CustomType>> session,
                                            ClientSession<CustomType, CustomType, CustomType, CustomType, byte, CustomTypeFunctions, FixedLenSerializer<CustomType, CustomType, CustomType, CustomType>> session2)
        {
            session2.Subscribe(new CustomType(23));
            session2.CompletePending(true);

            session2.Subscribe(new CustomType(24));
            session2.CompletePending(true);

            session2.PSubscribe(new CustomType(25));
            session2.CompletePending(true);

            session.Publish(new CustomType(23), new CustomType(2300));
            session.CompletePending(true);

            session.Publish(new CustomType(24), new CustomType(2400));
            session.CompletePending(true);

            session.Publish(new CustomType(25), new CustomType(2500));
            session.CompletePending(true);

            System.Threading.Thread.Sleep(1000);
        }

        async Task AsyncVarLenSamples(ClientSession<CustomType, CustomType, CustomType, CustomType, byte, CustomTypeFunctions, FixedLenSerializer<CustomType, CustomType, CustomType, CustomType>> session)
        {
            // By default, we flush async operations as soon as they are issued
            // To instead flush manually, set forceFlush = false in calls below

            session.Upsert(new CustomType(25), new CustomType(25 + 10000));

            var (status, output) = await session.ReadAsync(new CustomType(25));
            if (!status.Found || output.payload != 25 + 10000)
                throw new Exception("Error!");

            await session.DeleteAsync(new CustomType(25));

            (status, _) = await session.ReadAsync(new CustomType(25));
            if (!status.NotFound)
                throw new Exception("Error!");

            (status, _) = await session.ReadAsync(new CustomType(9999));
            if (!status.NotFound)
                throw new Exception("Error!");
        }
    }
}
