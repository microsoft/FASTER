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

        static Socket Connect() => RawSocket.Connect();

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

        /// <summary>A SpanByte with the given length header, followed by the given payload bytes.</summary>
        static byte[] SpanByteElement(int lengthHeader, byte[] payload)
        {
            var element = new List<byte>();
            element.AddRange(BitConverter.GetBytes(lengthHeader));
            element.AddRange(payload);
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

        static void AssertConnectionClosed(Socket socket) => RawSocket.AssertConnectionClosed(socket);

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
        static byte[] WebSocketFrame(byte[] payload, ulong? advertisedLength = null, byte opcode = 0x2, bool fin = true)
        {
            var length = advertisedLength ?? (ulong)payload.Length;
            var frame = new List<byte> { (byte)((fin ? 0x80 : 0x00) | opcode) };

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

        /// <summary>A websocket batch: a size field, a batch header, then messages with no serial numbers.</summary>
        static byte[] WebSocketBatch(int numMessages, byte[] messages)
        {
            var payload = new List<byte>();
            payload.AddRange(BitConverter.GetBytes(0));      // size
            payload.AddRange(BitConverter.GetBytes(0));      // seqNo
            payload.AddRange(BitConverter.GetBytes(numMessages));
            payload.AddRange(messages);
            return payload.ToArray();
        }

        static byte[] WebSocketMessage(MessageType type, params byte[][] elements)
        {
            var message = new List<byte> { (byte)type };
            foreach (var element in elements)
                message.AddRange(element);
            return message.ToArray();
        }

        /// <summary>
        /// A partial frame must be buffered until the remainder arrives, and must then be processed
        /// normally on the same connection.
        /// </summary>
        [Test]
        public void WebSocketPartialFrameIsBuffered()
        {
            using (var socket = ConnectWebSocket())
            {
                var frame = WebSocketFrame(WebSocketBatch(1,
                    WebSocketMessage(MessageType.Upsert, SpanByteElement(8), SpanByteElement(8))));

                socket.Send(frame, 0, 10, SocketFlags.None);
                Assert.IsFalse(socket.Poll(500_000, SelectMode.SelectRead),
                    "Server replied to, or closed, a connection whose frame had not fully arrived");

                socket.Send(frame, 10, frame.Length - 10, SocketFlags.None);
                Assert.Greater(socket.Receive(new byte[256]), 0);
            }

            AssertServerIsHealthy();
        }

        /// <summary>The reserved bits are only meaningful with a negotiated extension, and we negotiate none.</summary>
        [Test]
        public void WebSocketReservedBitsAreRejected()
        {
            // Otherwise valid, so that only the reserved bit can account for the rejection
            var frame = WebSocketFrame(WebSocketBatch(1,
                WebSocketMessage(MessageType.Upsert, SpanByteElement(8), SpanByteElement(8))));
            frame[0] |= 0b01000000;
            AssertWebSocketFrameRejected(frame);
        }

        /// <summary>
        /// Empty fragments contribute nothing to the decoded message size, so only a cap on the number of
        /// fragments bounds how much a single message may buffer.
        /// </summary>
        [Test]
        public void WebSocketTooManyFragmentsIsRejected()
        {
            var frames = new List<byte>();
            frames.AddRange(WebSocketFrame(Array.Empty<byte>(), fin: false));
            for (var i = 0; i < 1024; i++)
                frames.AddRange(WebSocketFrame(Array.Empty<byte>(), opcode: 0x0, fin: false));
            AssertWebSocketFrameRejected(frames.ToArray());
        }

        /// <summary>Subscriptions are disabled here, which is a protocol error rather than a null dereference.</summary>
        [Test]
        public void WebSocketSubscribeWithPubSubDisabledIsRejected()
            => AssertWebSocketFrameRejected(WebSocketFrame(WebSocketBatch(1,
                WebSocketMessage(MessageType.SubscribeKV, SpanByteElement(8), SpanByteElement(8)))));

        /// <summary>The same request on the binary protocol must likewise close only this connection.</summary>
        [Test]
        public void SubscribeWithPubSubDisabledIsRejected()
            => AssertRejected(BinaryBatch(1, Message(MessageType.SubscribeKV, SpanByteElement(8), SpanByteElement(8))));

        /// <summary>
        /// An upgrade request is buffered until its terminator arrives, so one that never terminates must
        /// be cut off rather than allowed to grow the receive buffer without bound.
        /// </summary>
        [Test]
        public void OversizedHttpUpgradeIsRejected()
        {
            using (var socket = Connect())
            {
                var filler = Encoding.UTF8.GetBytes(new string('x', 4096) + "\r\n");
                try
                {
                    socket.Send(Encoding.UTF8.GetBytes("GET / HTTP/1.1\r\nHost: localhost\r\n"));
                    for (var i = 0; i < 32; i++)
                        socket.Send(filler);
                }
                catch (SocketException)
                {
                    // Server closed the connection partway through, which is the behavior under test
                }

                AssertConnectionClosed(socket);
            }

            AssertServerIsHealthy();
        }

        /// <summary>
        /// A value carrying an eight-byte metadata header must be returned in full. Serializing only the
        /// payload left the metadata-sized tail of the reply holding whatever the response buffer last held.
        /// </summary>
        [Test]
        public void ValueMetadataIsNotPaddedWithResponseBufferContents()
        {
            const int metadataBit = 0x40000000;

            var filler = new byte[32];
            for (var i = 0; i < filler.Length; i++) filler[i] = 0xAA;

            var withMetadata = new byte[16];
            for (var i = 0; i < 8; i++) withMetadata[i] = 0x11;        // metadata, read as a positive long
            for (var i = 8; i < 16; i++) withMetadata[i] = 0x22;       // payload

            using var socket = Connect();

            // Leave the filler in the response buffer, then read the metadata-bearing value back into it
            RoundTrip(socket, SpanByteElement(4, BitConverter.GetBytes(1)), SpanByteElement(32, filler));
            var output = RoundTrip(socket, SpanByteElement(4, BitConverter.GetBytes(2)), SpanByteElement(metadataBit | 16, withMetadata));

            CollectionAssert.AreEqual(withMetadata, output);
        }

        /// <summary>
        /// A value too large for the response buffer is redirected to the heap, leaving nothing at the
        /// output cursor. Advancing the cursor by whatever the recycled buffer still held served the
        /// previous reply's bytes as this one's output and could run the cursor past the buffer.
        /// </summary>
        [Test]
        public void OversizedReadOutputIsRejected()
        {
            var small = new byte[4000];
            for (var i = 0; i < small.Length; i++) small[i] = 0xCD;

            var large = new byte[160_000];

            using (var socket = Connect())
            {
                // Leave a large output length in the pooled response buffers
                var smallKey = SpanByteElement(4, BitConverter.GetBytes(1));
                socket.Send(BinaryBatch(1, Message(MessageType.Upsert, smallKey, SpanByteElement(small.Length, small))));
                RawSocket.ReceiveBinaryBatch(socket);
                for (var i = 0; i < 4; i++)
                {
                    socket.Send(BinaryBatch(1, Message(MessageType.Read, smallKey, SpanByteElement(0))));
                    RawSocket.ReceiveBinaryBatch(socket);
                }

                var largeKey = SpanByteElement(4, BitConverter.GetBytes(2));
                socket.Send(BinaryBatch(1, Message(MessageType.Upsert, largeKey, SpanByteElement(large.Length, large))));
                RawSocket.ReceiveBinaryBatch(socket);

                socket.Send(BinaryBatch(1, Message(MessageType.Read, largeKey, SpanByteElement(0))));
                AssertConnectionClosed(socket);
            }

            AssertServerIsHealthy();
        }

        /// <summary>Upsert the pair, then read the key back, returning the serialized output of the read.</summary>
        static byte[] RoundTrip(Socket socket, byte[] key, byte[] value)
        {
            socket.Send(BinaryBatch(1, Message(MessageType.Upsert, key, value)));
            RawSocket.ReceiveBinaryBatch(socket);

            socket.Send(BinaryBatch(1, Message(MessageType.Read, key, SpanByteElement(0))));
            var reply = RawSocket.ReceiveBinaryBatch(socket);

            // [BatchHeader][message type][status][output length][output]
            var outputStart = BatchHeader.Size + 2 + sizeof(int);
            var outputLength = BitConverter.ToInt32(reply, BatchHeader.Size + 2);
            Assert.AreEqual(reply.Length, outputStart + outputLength, "Reply length did not match its output length");

            var output = new byte[outputLength];
            Array.Copy(reply, outputStart, output, 0, outputLength);
            return output;
        }
    }
}
