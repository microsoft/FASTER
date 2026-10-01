// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using FASTER.common;
using FASTER.server;
using NUnit.Framework;

namespace FASTER.remote.test
{
    /// <summary>
    /// Every payload in this fixture is derived from an input that previously caused an unhandled
    /// exception on the socket engine thread, terminating the whole server process. The server must
    /// instead close only the offending connection and keep serving other clients.
    /// </summary>
    [TestFixture]
    public class MalformedRequestTests
    {
        VarLenServer server;

        [SetUp]
        public void Setup()
        {
            server = TestUtils.CreateVarLenServer(TestContext.CurrentContext.TestDirectory + "/MalformedRequestTests", disablePubSub: true);
            server.Start();
        }

        [TearDown]
        public void TearDown() => server.Dispose();

        static Socket Connect()
        {
            var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true, ReceiveTimeout = 10000 };
            socket.Connect(TestUtils.Address, TestUtils.Port);
            return socket;
        }

        /// <summary>
        /// Wrap a batch in the binary framing: a negated size field followed by a batch header.
        /// </summary>
        static byte[] BinaryBatch(int numMessages, byte[] messages)
        {
            var payload = new List<byte>();
            payload.AddRange(BitConverter.GetBytes(0));                                      // BatchHeader.SeqNo
            payload.AddRange(BitConverter.GetBytes((numMessages << 8) | (int)WireFormat.DefaultVarLenKV));
            payload.AddRange(messages);

            var packet = new List<byte>();
            packet.AddRange(BitConverter.GetBytes(-payload.Count));
            packet.AddRange(payload);
            return packet.ToArray();
        }

        static byte[] Message(MessageType type, params byte[][] elements)
        {
            var message = new List<byte> { (byte)type };
            message.AddRange(BitConverter.GetBytes(0L));                                     // serial number
            foreach (var element in elements)
                message.AddRange(element);
            return message.ToArray();
        }

        /// <summary>A SpanByte with the given length header, followed by the given number of payload bytes.</summary>
        static byte[] SpanByteElement(int lengthHeader, int payloadBytes)
        {
            var element = new List<byte>();
            element.AddRange(BitConverter.GetBytes(lengthHeader));
            element.AddRange(new byte[payloadBytes]);
            return element.ToArray();
        }

        /// <summary>A well-formed, self-consistent SpanByte.</summary>
        static byte[] SpanByteElement(int payloadBytes) => SpanByteElement(payloadBytes, payloadBytes);

        /// <summary>A batch carrying no messages: enough to establish a session, and drawing no reply.</summary>
        static byte[] SessionEstablishingBatch() => BinaryBatch(0, Array.Empty<byte>());

        /// <summary>
        /// Send the packets, then verify the server closed this connection and is otherwise healthy.
        /// </summary>
        void AssertRejected(params byte[][] packets)
        {
            using (var socket = Connect())
            {
                foreach (var packet in packets)
                {
                    socket.Send(packet);
                    Thread.Sleep(50);
                }

                AssertConnectionClosed(socket);
            }

            AssertServerIsHealthy();
        }

        /// <summary>
        /// Drain any replies and require the server to end the connection, either cleanly or by reset.
        /// </summary>
        static void AssertConnectionClosed(Socket socket)
        {
            var buffer = new byte[4096];
            var deadline = DateTime.UtcNow.AddSeconds(30);
            while (DateTime.UtcNow < deadline)
            {
                try
                {
                    if (socket.Receive(buffer) == 0) return;
                }
                catch (SocketException e) when (e.SocketErrorCode != SocketError.TimedOut)
                {
                    return;
                }
            }
            Assert.Fail("Server did not close the connection that sent a malformed request");
        }

        /// <summary>A well-formed client round trip must still succeed after the malformed request.</summary>
        static void AssertServerIsHealthy()
        {
            using var client = new VarLenMemoryClient();
            using var session = client.GetSession();
            var key = new Memory<int>(new int[] { 42, 1 });
            var value = new Memory<int>(new int[] { 42 });
            session.Upsert(key, value);
            session.CompletePending(true);
            session.Read(key, userContext: 42);
            session.CompletePending(true);
        }

        /// <summary>
        /// A SpanByte whose length field claims far more data than was received must not be dereferenced.
        /// </summary>
        [Test]
        public void SpanByteLengthBeyondBufferIsRejected()
            => AssertRejected(BinaryBatch(1, Message(MessageType.Upsert, SpanByteElement(0x3FFFFFFF, 0), SpanByteElement(0))));

        /// <summary>A length field that is only modestly too long must be rejected just the same.</summary>
        [Test]
        public void SpanByteLengthBeyondBatchIsRejected()
            => AssertRejected(BinaryBatch(1, Message(MessageType.Upsert, SpanByteElement(100, 8), SpanByteElement(8))));

        /// <summary>
        /// A SpanByte flagged as unserialized holds a raw pointer in place of inline data, so it must
        /// never be accepted from the network.
        /// </summary>
        [Test]
        public void UnserializedSpanByteIsRejected()
            => AssertRejected(BinaryBatch(1, Message(MessageType.Upsert, SpanByteElement(unchecked((int)0x80000008), 8), SpanByteElement(8))));

        /// <summary>A batch that claims more messages than its payload holds must not read past the batch.</summary>
        [Test]
        public void BatchClaimingTooManyMessagesIsRejected()
            => AssertRejected(BinaryBatch(1000, Message(MessageType.Upsert, SpanByteElement(8), SpanByteElement(8))));

        /// <summary>A batch header that was not fully supplied must not be read as one.</summary>
        [Test]
        public void TruncatedBatchHeaderIsRejected()
        {
            var truncated = new List<byte>();
            truncated.AddRange(BitConverter.GetBytes(-4));
            truncated.AddRange(BitConverter.GetBytes(0));
            AssertRejected(SessionEstablishingBatch(), truncated.ToArray());
        }

        /// <summary>A message header that was not fully supplied must not be read as one.</summary>
        [Test]
        public void TruncatedMessageHeaderIsRejected()
            => AssertRejected(BinaryBatch(1, new byte[] { (byte)MessageType.Upsert, 0, 0 }));

        /// <summary>An unknown message type is a protocol error, not a reason to terminate the server.</summary>
        [Test]
        public void UnknownMessageTypeIsRejected()
            => AssertRejected(BinaryBatch(1, Message((MessageType)200, SpanByteElement(8))));

        /// <summary>
        /// int.MinValue as the size field negates back to itself in 32-bit arithmetic, which previously
        /// drove the read position negative and corrupted the receive buffer bookkeeping.
        /// </summary>
        [Test]
        public void IntMinValueSizeFieldIsRejected()
        {
            var hostile = new List<byte>();
            hostile.AddRange(BitConverter.GetBytes(int.MinValue));
            hostile.AddRange(BitConverter.GetBytes(0));
            hostile.AddRange(BitConverter.GetBytes((int)WireFormat.DefaultVarLenKV));
            hostile.AddRange(new byte[4]);
            AssertRejected(SessionEstablishingBatch(), hostile.ToArray());
        }

        /// <summary>
        /// A size field of 0x80000001 decodes to int.MaxValue, for which "size + sizeof(int)" previously
        /// overflowed to a negative value and so passed the "message fully received" check.
        /// </summary>
        [Test]
        public void SizeFieldOverflowingTheLengthCheckIsRejected()
        {
            var hostile = new List<byte>();
            hostile.AddRange(BitConverter.GetBytes(unchecked((int)0x80000001)));
            hostile.AddRange(new byte[12]);
            AssertRejected(SessionEstablishingBatch(), hostile.ToArray());
        }

        /// <summary>A size field without the protocol marker set decodes to a negative size.</summary>
        [Test]
        public void NonNegatedSizeFieldIsRejected()
        {
            var hostile = new List<byte>();
            hostile.AddRange(BitConverter.GetBytes(16));
            hostile.AddRange(new byte[16]);
            AssertRejected(SessionEstablishingBatch(), hostile.ToArray());
        }

        const string WebSocketKey = "dGhlIHNhbXBsZSBub25jZQ==";

        static byte[] WebSocketUpgrade() => Encoding.UTF8.GetBytes(
            "GET / HTTP/1.1\r\n" +
            "Host: localhost\r\n" +
            "Upgrade: websocket\r\n" +
            "Connection: Upgrade\r\n" +
            "Sec-WebSocket-Key: " + WebSocketKey + "\r\n" +
            "Sec-WebSocket-Version: 13\r\n\r\n");

        /// <summary>Build a masked client frame, optionally overriding the advertised payload length.</summary>
        static byte[] WebSocketFrame(byte[] payload, ulong? advertisedLength = null, byte opcode = 0x2)
        {
            var length = advertisedLength ?? (ulong)payload.Length;
            var frame = new List<byte> { (byte)(0x80 | opcode) };

            if (advertisedLength == null && length < 126)
                frame.Add((byte)(0x80 | length));
            else if (advertisedLength == null && length < 65536)
            {
                frame.Add(0x80 | 126);
                frame.Add((byte)(length >> 8));
                frame.Add((byte)length);
            }
            else
            {
                frame.Add(0x80 | 127);
                for (var i = 7; i >= 0; i--)
                    frame.Add((byte)(length >> (i * 8)));
            }

            var mask = new byte[] { 1, 2, 3, 4 };
            frame.AddRange(mask);
            for (var i = 0; i < payload.Length; i++)
                frame.Add((byte)(payload[i] ^ mask[i % 4]));
            return frame.ToArray();
        }

        static Socket ConnectWebSocket()
        {
            var socket = Connect();
            socket.Send(WebSocketUpgrade());

            var buffer = new byte[1024];
            var handshake = Encoding.UTF8.GetString(buffer, 0, socket.Receive(buffer));
            StringAssert.StartsWith("HTTP/1.1 101", handshake);

            using var sha1 = SHA1.Create();
            var accept = Convert.ToBase64String(sha1.ComputeHash(
                Encoding.UTF8.GetBytes(WebSocketKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")));
            StringAssert.Contains(accept, handshake);

            return socket;
        }

        /// <summary>Complete the handshake, send the frame, and verify only this connection is affected.</summary>
        void AssertWebSocketFrameRejected(byte[] frame)
        {
            using (var socket = ConnectWebSocket())
            {
                socket.Send(frame);
                AssertConnectionClosed(socket);
            }

            AssertServerIsHealthy();
        }

        /// <summary>
        /// A 64-bit extended payload length must be validated before it is narrowed to an allocation size.
        /// </summary>
        [Test]
        public void WebSocketOversizedExtendedLengthIsRejected()
            => AssertWebSocketFrameRejected(WebSocketFrame(new byte[] { 0 }, advertisedLength: 0x7FFFFFFE));

        /// <summary>A 64-bit length whose high bits are set must not be narrowed into a negative length.</summary>
        [Test]
        public void WebSocketNegativeExtendedLengthIsRejected()
            => AssertWebSocketFrameRejected(WebSocketFrame(new byte[] { 0 }, advertisedLength: ulong.MaxValue));

        /// <summary>A frame carrying less than a batch header must not be parsed as one.</summary>
        [Test]
        public void WebSocketUndersizedBatchIsRejected()
            => AssertWebSocketFrameRejected(WebSocketFrame(new byte[] { 1, 2, 3 }));

        /// <summary>Client frames must be masked; an unmasked frame previously yielded a bogus length.</summary>
        [Test]
        public void WebSocketUnmaskedFrameIsRejected()
            => AssertWebSocketFrameRejected(new byte[] { 0x82, 0x05, 1, 2, 3, 4, 5 });

        /// <summary>A websocket batch claiming more messages than it carries must not be over-read.</summary>
        [Test]
        public void WebSocketBatchClaimingTooManyMessagesIsRejected()
        {
            var payload = new List<byte>();
            payload.AddRange(BitConverter.GetBytes(0));      // size
            payload.AddRange(BitConverter.GetBytes(0));      // seqNo
            payload.AddRange(BitConverter.GetBytes(1000));   // numMessages
            AssertWebSocketFrameRejected(WebSocketFrame(payload.ToArray()));
        }

        /// <summary>A websocket SpanByte length must be bounded by the decoded message, as in the binary path.</summary>
        [Test]
        public void WebSocketSpanByteLengthBeyondBufferIsRejected()
        {
            var payload = new List<byte>();
            payload.AddRange(BitConverter.GetBytes(0));      // size
            payload.AddRange(BitConverter.GetBytes(0));      // seqNo
            payload.AddRange(BitConverter.GetBytes(1));      // numMessages
            payload.Add((byte)MessageType.Upsert);
            payload.AddRange(SpanByteElement(0x3FFFFFFF, 0));
            payload.AddRange(SpanByteElement(0));
            AssertWebSocketFrameRejected(WebSocketFrame(payload.ToArray()));
        }

        /// <summary>A partial frame must leave the server waiting for more data, not reading past it.</summary>
        [Test]
        public void WebSocketPartialFrameIsBuffered()
        {
            using (var socket = ConnectWebSocket())
            {
                // Announce a 200-byte payload but send only the first 20 bytes of the frame
                var frame = WebSocketFrame(new byte[200]);
                socket.Send(frame, 0, 20, SocketFlags.None);
                Thread.Sleep(200);
            }

            AssertServerIsHealthy();
        }
    }
}
