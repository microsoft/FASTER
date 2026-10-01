// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Net.Sockets;
using FASTER.common;
using FASTER.core;
using FASTER.server;
using NUnit.Framework;

namespace FASTER.remote.test
{
    /// <summary>
    /// The fixed-length wire format reads blittable elements straight out of the receive buffer, so it
    /// needs the same bounds checking as the variable-length format: a message that stops short of the
    /// element it promises must close only the offending connection.
    /// </summary>
    [TestFixture]
    public class MalformedFixedLenRequestTests
    {
        FixedLenServer<long, long, long, long, SimpleFunctions<long, long, long>> server;

        [SetUp]
        public void Setup()
        {
            server = TestUtils.CreateFixedLenServer(
                TestContext.CurrentContext.TestDirectory + "/MalformedFixedLenRequestTests", (a, b) => a + b, disablePubSub: true);
            server.Start();
        }

        [TearDown]
        public void TearDown() => server.Dispose();

        static byte[] BinaryBatch(int numMessages, byte[] messages)
        {
            var payload = new List<byte>();
            payload.AddRange(BitConverter.GetBytes(0));                                      // BatchHeader.SeqNo
            payload.AddRange(BitConverter.GetBytes((numMessages << 8) | (int)WireFormat.DefaultFixedLenKV));
            payload.AddRange(messages);

            var packet = new List<byte>();
            packet.AddRange(BitConverter.GetBytes(-payload.Count));
            packet.AddRange(payload);
            return packet.ToArray();
        }

        static byte[] Message(MessageType type, params byte[] elementBytes)
        {
            var message = new List<byte> { (byte)type };
            message.AddRange(BitConverter.GetBytes(0L));                                     // serial number
            message.AddRange(elementBytes);
            return message.ToArray();
        }

        void AssertRejected(byte[] packet)
        {
            using (var socket = RawSocket.Connect())
            {
                socket.Send(packet);
                RawSocket.AssertConnectionClosed(socket);
            }

            AssertServerIsHealthy();
        }

        static void AssertServerIsHealthy()
        {
            using var client = new FixedLenClient<long, long>();
            using var session = client.GetSession();
            session.Upsert(10, 23);
            session.CompletePending(true);
            session.Read(10, userContext: 23);
            session.CompletePending(true);
        }

        /// <summary>An Upsert whose key stops short of eight bytes must not be read as a long.</summary>
        [Test]
        public void TruncatedKeyIsRejected()
            => AssertRejected(BinaryBatch(1, Message(MessageType.Upsert, 1, 2, 3, 4)));

        /// <summary>An Upsert carrying a key but no value must not read the value past the batch.</summary>
        [Test]
        public void TruncatedValueIsRejected()
            => AssertRejected(BinaryBatch(1, Message(MessageType.Upsert, new byte[8 + 4])));

        /// <summary>A Read whose input is missing must not be read past the batch either.</summary>
        [Test]
        public void TruncatedInputIsRejected()
            => AssertRejected(BinaryBatch(1, Message(MessageType.Read, new byte[8])));

        /// <summary>A batch claiming more messages than it carries must not be over-read.</summary>
        [Test]
        public void BatchClaimingTooManyMessagesIsRejected()
            => AssertRejected(BinaryBatch(1000, Message(MessageType.Upsert, new byte[16])));

        /// <summary>An unknown message type is a protocol error, not a reason to terminate the server.</summary>
        [Test]
        public void UnknownMessageTypeIsRejected()
            => AssertRejected(BinaryBatch(1, Message((MessageType)200, new byte[16])));
    }
}
