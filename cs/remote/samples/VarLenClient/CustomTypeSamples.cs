// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
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
            using var session = client.NewSession(new CustomTypeFunctions(), WireFormat.DefaultVarLenKV);
            using var subSession = client.NewSession(new CustomTypeFunctions(), WireFormat.DefaultVarLenKV);

            // Explicit version of NewSession call, where you provide all types, callback functions, and serializer
            // using var session = client.NewSession<long, long, long, Functions, BlittableParameterSerializer<long, long, long, long>>(new Functions(), new BlittableParameterSerializer<long, long, long, long>());

            // Samples using sync client API
            SyncVarLenSamples(session);

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

        void SyncVarLenSubscriptionSamples(ClientSession<CustomType, CustomType, CustomType, CustomType, byte, CustomTypeFunctions, FixedLenSerializer<CustomType, CustomType, CustomType, CustomType>> session,
                                            ClientSession<CustomType, CustomType, CustomType, CustomType, byte, CustomTypeFunctions, FixedLenSerializer<CustomType, CustomType, CustomType, CustomType>> session2)
        {
            session2.SubscribeKV(new CustomType(23));
            session2.CompletePending(true);

            session.Upsert(new CustomType(23), new CustomType(2300));
            session.CompletePending(true);

            System.Threading.Thread.Sleep(1000);
        }


        async Task AsyncVarLenSamples(ClientSession<CustomType, CustomType, CustomType, CustomType, byte, CustomTypeFunctions, FixedLenSerializer<CustomType, CustomType, CustomType, CustomType>> session)
        {
            // By default, we flush async operations as soon as they are issued
            // To instead flush manually, set forceFlush = false in calls below

            session.Upsert(new CustomType(25), new CustomType(25 + 10000));

            var (status, output) = await session.ReadAsync(new CustomType(25));
            if (status != Status.OK || output.payload != 25 + 10000)
                throw new Exception("Error!");

            await session.DeleteAsync(new CustomType(25));

            (status, _) = await session.ReadAsync(new CustomType(25));
            if (status != Status.NOTFOUND)
                throw new Exception("Error!");

            (status, _) = await session.ReadAsync(new CustomType(9999));
            if (status != Status.NOTFOUND)
                throw new Exception("Error!");
        }
    }
}
