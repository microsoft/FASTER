// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FASTER.common;

namespace FASTER.client
{
    /// <summary>
    /// Client session wrapper
    /// </summary>
    /// <typeparam name="Key">Key</typeparam>
    /// <typeparam name="Value">Value</typeparam>
    /// <typeparam name="Input">Input</typeparam>
    /// <typeparam name="Output">Output</typeparam>
    /// <typeparam name="Context">Context</typeparam>
    /// <typeparam name="Functions">Functions</typeparam>
    /// <typeparam name="ParameterSerializer">Parameter Serializer</typeparam>
    public unsafe sealed partial class ClientSession<Key, Value, Input, Output, Context, Functions, ParameterSerializer> : IDisposable
            where Functions : ICallbackFunctions<Key, Value, Input, Output, Context>
            where ParameterSerializer : IClientSerializer<Key, Value, Input, Output>
    {
        readonly NetworkSender messageManager;
        readonly Functions functions;
        readonly ParameterSerializer serializer;
        readonly Socket sendSocket;
        readonly HeaderReaderWriter hrw;
        readonly int bufferSize;
        readonly WireFormat wireFormat;
        readonly MaxSizeSettings maxSizeSettings;
        private bool subscriptionSession;

        bool disposed;
        ReusableObject<SeaaBuffer> sendObject;
        byte* offset;
        int numMessages;
        int numPendingBatches;

        readonly ElasticCircularBuffer<(Key, Value, Context)> upsertQueue;
        readonly ElasticCircularBuffer<(Key, Input, Output, Context)> readrmwQueue;
        readonly ElasticCircularBuffer<(Key, Value, Context)> pubsubQueue;
        readonly ElasticCircularBuffer<TaskCompletionSource<(Status, Output)>> tcsQueue;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="address">IP address</param>
        /// <param name="port">Port</param>
        /// <param name="functions">Client callback functions</param>
        /// <param name="wireFormat"></param>
        /// <param name="serializer">Serializer</param>
        /// <param name="maxSizeSettings">Size settings</param>
        public ClientSession(string address, int port, Functions functions, WireFormat wireFormat, ParameterSerializer serializer, MaxSizeSettings maxSizeSettings)
        {
            this.functions = functions;
            this.serializer = serializer;
            this.wireFormat = wireFormat;
            this.maxSizeSettings = maxSizeSettings ?? new MaxSizeSettings();
            this.bufferSize = BufferSizeUtils.ClientBufferSize(this.maxSizeSettings);
            this.messageManager = new NetworkSender(bufferSize);
            this.disposed = false;
            this.subscriptionSession = false;

            upsertQueue = new ElasticCircularBuffer<(Key, Value, Context)>();
            readrmwQueue = new ElasticCircularBuffer<(Key, Input, Output, Context)>();
            pubsubQueue = new ElasticCircularBuffer<(Key, Value, Context)>();
            tcsQueue = new ElasticCircularBuffer<TaskCompletionSource<(Status, Output)>>();

            numPendingBatches = 0;
            sendObject = messageManager.GetReusableSeaaBuffer();
            offset = sendObject.obj.bufferPtr + sizeof(int) + BatchHeader.Size;
            numMessages = 0;
            sendSocket = GetSendSocket(address, port);
        }

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="desiredValue">Desired value</param>
        /// <param name="userContext">User context</param>
        /// <param name="serialNo">Serial number</param>
        /// <returns>Status of operation</returns>
        public Status Upsert(ref Key key, ref Value desiredValue, Context userContext = default, long serialNo = 0)
            => InternalUpsert(MessageType.Upsert, ref key, ref desiredValue, userContext, serialNo);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="desiredValue">Desired value</param>
        /// <param name="userContext">User context</param>
        /// <param name="serialNo">Serial number</param>
        /// <returns>Status of operation</returns>
        public Status Upsert(Key key, Value desiredValue, Context userContext = default, long serialNo = 0)
            => InternalUpsert(MessageType.Upsert, ref key, ref desiredValue, userContext, serialNo);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="output">Output</param>
        /// <param name="userContext">User context</param>
        /// <param name="serialNo">Serial number</param>
        /// <returns>Status of operation</returns>
        public Status Read(ref Key key, ref Input input, ref Output output, Context userContext = default, long serialNo = 0)
            => InternalRead(MessageType.Read, ref key, ref input, ref output, userContext, serialNo);

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="output">Output</param>
        /// <param name="userContext">User context</param>
        /// <param name="serialNo">Serial number</param>
        /// <returns>Status of operation</returns>
        public Status Read(Key key, Input input, out Output output, Context userContext = default, long serialNo = 0)
        {
            output = default;
            return InternalRead(MessageType.Read, ref key, ref input, ref output, userContext, serialNo);
        }

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="userContext">User context</param>
        /// <param name="serialNo">Serial number</param>
        /// <returns>Status of operation</returns>
        public (Status, Output) Read(Key key, Input input = default, Context userContext = default, long serialNo = 0)
        {
            Output output = default;
            return (InternalRead(MessageType.Read, ref key, ref input, ref output, userContext, serialNo), output);
        }

        /// <summary>
        /// RMW (read-modify-write) operation
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="userContext">User context</param>
        /// <param name="serialNo">Serial number</param>
        /// <returns>Status of operation</returns>
        public Status RMW(ref Key key, ref Input input, Context userContext = default, long serialNo = 0)
        {
            Output output = default;
            return InternalRMW(MessageType.RMW, ref key, ref input, ref output, userContext, serialNo);
        }

        /// <summary>
        /// RMW (read-modify-write) operation
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="output">Output</param>
        /// <param name="userContext">User context</param>
        /// <param name="serialNo">Serial number</param>
        /// <returns>Status of operation</returns>
        public Status RMW(ref Key key, ref Input input, ref Output output, Context userContext = default, long serialNo = 0)
            => InternalRMW(MessageType.RMW, ref key, ref input, ref output, userContext, serialNo);

        /// <summary>
        /// RMW (read-modify-write) operation
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="userContext">User context</param>
        /// <param name="serialNo">Serial number</param>
        /// <returns>Status of operation</returns>
        public Status RMW(Key key, Input input, Context userContext = default, long serialNo = 0)
        {
            Output output = default;
            return InternalRMW(MessageType.RMW, ref key, ref input, ref output, userContext, serialNo);
        }

        /// <summary>
        /// RMW (read-modify-write) operation
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="output">Output</param>
        /// <param name="userContext">User context</param>
        /// <param name="serialNo">Serial number</param>
        /// <returns>Status of operation</returns>
        public Status RMW(Key key, Input input, out Output output, Context userContext = default, long serialNo = 0)
        {
            output = default;
            return InternalRMW(MessageType.RMW, ref key, ref input, ref output, userContext, serialNo);
        }

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="userContext">User context</param>
        /// <param name="serialNo">Serial number</param>
        /// <returns>Status of operation</returns>
        public Status Delete(ref Key key, Context userContext = default, long serialNo = 0)
            => InternalDelete(MessageType.Delete, ref key, userContext, serialNo);

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="userContext">User context</param>
        /// <param name="serialNo">Serial number</param>
        /// <returns>Status of operation</returns>
        public Status Delete(Key key, Context userContext = default, long serialNo = 0)
            => InternalDelete(MessageType.Delete, ref key, userContext, serialNo);

        /// <summary>
        /// SubscribeKV operation
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="userContext">User context</param>
        /// <param name="serialNo">Serial number</param>
        /// <returns>Status of operation</returns>
        public void SubscribeKV(Key key, Input input = default, Context userContext = default, long serialNo = 0)
            => InternalSubscribeKV(MessageType.SubscribeKV, ref key, ref input, userContext, serialNo);

        /// <summary>
        /// PSubscribeKV operation
        /// </summary>
        /// <param name="prefix">Key</param>
        /// <param name="input">Input</param>
        /// <param name="userContext">User context</param>
        /// <param name="serialNo">Serial number</param>
        /// <returns>Status of operation</returns>
        public void PSubscribeKV(Key prefix, Input input = default, Context userContext = default, long serialNo = 0)
            => InternalSubscribeKV(MessageType.PSubscribeKV, ref prefix, ref input, userContext, serialNo);

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="desiredValue">Desired value</param>
        /// <param name="userContext">User context</param>
        /// <param name="serialNo">Serial number</param>
        /// <returns>Status of operation</returns>
        public Status Publish(Key key, Value desiredValue, Context userContext = default, long serialNo = 0)
            => InternalPublish(MessageType.Publish, ref key, ref desiredValue, userContext, serialNo);


        /// <summary>
        /// SubscribeKV operation
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input</param>
        /// <param name="userContext">User context</param>
        /// <param name="serialNo">Serial number</param>
        /// <returns>Status of operation</returns>
        public void Subscribe(Key key, Context userContext = default, long serialNo = 0)
            => InternalSubscribe(MessageType.Subscribe, ref key, userContext, serialNo);

        /// <summary>
        /// PSubscribe operation
        /// </summary>
        /// <param name="prefix">Key</param>
        /// <param name="input">Input</param>
        /// <param name="userContext">User context</param>
        /// <param name="serialNo">Serial number</param>
        /// <returns>Status of operation</returns>
        public void PSubscribe(Key prefix, Context userContext = default, long serialNo = 0)
            => InternalSubscribe(MessageType.PSubscribe, ref prefix, userContext, serialNo);


        /// <summary>
        /// Flush current buffer of outgoing messages. Does not wait for responses.
        /// </summary>
        public void Flush()
        {
            if (offset > sendObject.obj.bufferPtr + sizeof(int) + BatchHeader.Size)
            {
                int payloadSize = (int)(offset - sendObject.obj.bufferPtr);

                ((BatchHeader*)(sendObject.obj.bufferPtr + sizeof(int)))->SetNumMessagesProtocol(numMessages, wireFormat);
                Interlocked.Increment(ref numPendingBatches);

                // Set packet size in header
                *(int*)sendObject.obj.bufferPtr = -(payloadSize - sizeof(int));

                try
                {
                    messageManager.Send(sendSocket, sendObject, 0, payloadSize);
                }
                catch
                {
                    Dispose();
                    throw;
                }
                sendObject = messageManager.GetReusableSeaaBuffer();
                offset = sendObject.obj.bufferPtr + sizeof(int) + BatchHeader.Size;
                numMessages = 0;
            }
        }

        /// <summary>
        /// Flush current buffer of outgoing messages. Spin-wait for all responses to be received and process them.
        /// </summary>
        public void CompletePending(bool wait = true)
        {
            Flush();
            if (wait)
                while (numPendingBatches > 0)
                {
                    Thread.Yield();
                }
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            disposed = true;
            sendObject.Dispose();
            sendSocket.Dispose();
            messageManager.Dispose();
        }

        /// <summary>
        /// Dispose
        /// </summary>
        /// <param name="completePending">Complete pending operations before dispose</param>
        public void Dispose(bool completePending)
        {
            if (completePending)
                CompletePending(true);
            Dispose();
        }


        int lastSeqNo = -1;
        readonly Dictionary<int, (Key, Value, Context)> pubsubPendingContext = new();
        readonly Dictionary<int, (Key, Input, Output, Context)> readRmwPendingContext = new();
        readonly Dictionary<int, TaskCompletionSource<(Status, Output)>> readRmwPendingTcs = new();

        internal void ProcessReplies(byte[] buf, int offset)
        {
            Output defaultOutput = default;
            fixed (byte* b = &buf[offset])
            {
                var src = b;
                var seqNo = ((BatchHeader*)src)->SeqNo;
                var count = ((BatchHeader*)src)->NumMessages;
                if (seqNo != lastSeqNo + 1)
                    throw new Exception("Out of order message within session");
                lastSeqNo = seqNo;

                src += BatchHeader.Size;

                for (int i = 0; i < count; i++)
                {
                    switch ((MessageType)(*src++))
                    {
                        case MessageType.Upsert:
                            {
                                var status = ReadStatus(ref src);
                                (Key, Value, Context) result = upsertQueue.Dequeue();
                                functions.UpsertCompletionCallback(ref result.Item1, ref result.Item2, result.Item3);
                                break;
                            }
                        case MessageType.UpsertAsync:
                            {
                                var status = ReadStatus(ref src);
                                (Key, Value, Context) result = upsertQueue.Dequeue();
                                var tcs = tcsQueue.Dequeue();
                                tcs.SetResult((status, default));
                                break;
                            }
                        case MessageType.Read:
                            {
                                var status = ReadStatus(ref src);
                                var result = readrmwQueue.Dequeue();
                                if (status == Status.OK)
                                {
                                    result.Item3 = serializer.ReadOutput(ref src);
                                    functions.ReadCompletionCallback(ref result.Item1, ref result.Item2, ref result.Item3, result.Item4, status);
                                }
                                else if (status == Status.PENDING)
                                {
                                    var p = hrw.ReadPendingSeqNo(ref src);
                                    readRmwPendingContext.Add(p, result);
                                }
                                else
                                    functions.ReadCompletionCallback(ref result.Item1, ref result.Item2, ref defaultOutput, result.Item4, status);

                                break;
                            }
                        case MessageType.ReadAsync:
                            {
                                var status = ReadStatus(ref src);
                                var result = readrmwQueue.Dequeue();
                                var tcs = tcsQueue.Dequeue();
                                if (status == Status.OK)
                                    tcs.SetResult((status, serializer.ReadOutput(ref src)));
                                else if (status == Status.PENDING)
                                {
                                    var p = hrw.ReadPendingSeqNo(ref src);
                                    readRmwPendingTcs.Add(p, tcs);
                                }
                                else
                                    tcs.SetResult((status, default));
                                break;
                            }
                        case MessageType.RMW:
                            {
                                var status = ReadStatus(ref src);
                                var result = readrmwQueue.Dequeue();
                                if (status == Status.OK || status == Status.NOTFOUND)
                                {
                                    result.Item3 = serializer.ReadOutput(ref src);
                                    functions.RMWCompletionCallback(ref result.Item1, ref result.Item2, ref result.Item3, result.Item4, status);
                                }
                                else if (status == Status.PENDING)
                                {
                                    var p = hrw.ReadPendingSeqNo(ref src);
                                    readRmwPendingContext.Add(p, result);
                                }
                                else
                                    functions.RMWCompletionCallback(ref result.Item1, ref result.Item2, ref defaultOutput, result.Item4, status);
                                break;
                            }
                        case MessageType.RMWAsync:
                            {
                                var status = ReadStatus(ref src);
                                var result = readrmwQueue.Dequeue();
                                var tcs = tcsQueue.Dequeue();
                                if (status == Status.OK || status == Status.NOTFOUND)
                                    tcs.SetResult((status, serializer.ReadOutput(ref src)));
                                else if (status == Status.PENDING)
                                {
                                    var p = hrw.ReadPendingSeqNo(ref src);
                                    readRmwPendingTcs.Add(p, tcs);
                                }
                                else
                                    tcs.SetResult((status, default));
                                break;
                            }
                        case MessageType.Delete:
                            {
                                var status = ReadStatus(ref src);
                                (Key, Value, Context) result = upsertQueue.Dequeue();
                                functions.DeleteCompletionCallback(ref result.Item1, result.Item3);
                                break;
                            }
                        case MessageType.DeleteAsync:
                            {
                                var status = ReadStatus(ref src);
                                (Key, Value, Context) result = upsertQueue.Dequeue();
                                var tcs = tcsQueue.Dequeue();
                                tcs.SetResult((status, default));
                                break;
                            }
                        case MessageType.SubscribeKV:
                            {
                                var status = ReadStatus(ref src);
                                var p = hrw.ReadPendingSeqNo(ref src);
                                if (status == Status.OK)
                                {
                                    readRmwPendingContext.TryGetValue(p, out var result);
                                    result.Item3 = serializer.ReadOutput(ref src);
                                    functions.SubscribeKVCallback(ref result.Item1, ref result.Item2, ref result.Item3, result.Item4, Status.OK);
                                }
                                else if (status == Status.NOTFOUND)
                                {
                                    readRmwPendingContext.TryGetValue(p, out var result);
                                    functions.SubscribeKVCallback(ref result.Item1, ref result.Item2, ref defaultOutput, result.Item4, Status.NOTFOUND);
                                }
                                else if (status == Status.PENDING)
                                {
                                    var result = readrmwQueue.Dequeue();
                                    readRmwPendingContext.Add(p, result);
                                }
                                else
                                {
                                    throw new Exception("Unexpected status of SubscribeKV");
                                }
                                break;
                            }
                        case MessageType.PSubscribeKV:
                            {
                                var status = ReadStatus(ref src);
                                var p = hrw.ReadPendingSeqNo(ref src);
                                if (status == Status.OK)
                                {
                                    readRmwPendingContext.TryGetValue(p, out var result);
                                    result.Item1 = serializer.ReadKey(ref src);
                                    result.Item3 = serializer.ReadOutput(ref src);
                                    functions.SubscribeKVCallback(ref result.Item1, ref result.Item2, ref result.Item3, result.Item4, Status.OK);
                                }
                                else if (status == Status.NOTFOUND)
                                {
                                    readRmwPendingContext.TryGetValue(p, out var result);
                                    result.Item1 = serializer.ReadKey(ref src);
                                    functions.SubscribeKVCallback(ref result.Item1, ref result.Item2, ref defaultOutput, result.Item4, Status.NOTFOUND);
                                }
                                else if (status == Status.PENDING)
                                {
                                    var result = readrmwQueue.Dequeue();
                                    readRmwPendingContext.Add(p, result);
                                }
                                else
                                {
                                    throw new Exception("Unexpected status of SubscribeKV");
                                }
                                break;
                            }
                        case MessageType.Publish:
                            {
                                var status = ReadStatus(ref src);
                                (Key, Value, Context) result = upsertQueue.Dequeue();
                                functions.PublishCompletionCallback(ref result.Item1, ref result.Item2, result.Item3);
                                break;
                            }
                        case MessageType.Subscribe:
                            {
                                var status = ReadStatus(ref src);
                                var p = hrw.ReadPendingSeqNo(ref src);
                                if (status == Status.OK)
                                {
                                    pubsubPendingContext.TryGetValue(p, out var result);
                                    result.Item2 = serializer.ReadValue(ref src);
                                    functions.SubscribeCallback(ref result.Item1, ref result.Item2, result.Item3);
                                }
                                else if (status == Status.PENDING)
                                {
                                    var result = pubsubQueue.Dequeue();
                                    pubsubPendingContext.Add(p, result);
                                }
                                else
                                {
                                    throw new Exception("Unexpected status of SubscribeKV");
                                }
                                break;
                            }
                        case MessageType.PSubscribe:
                            {
                                var status = ReadStatus(ref src);
                                var p = hrw.ReadPendingSeqNo(ref src);
                                if (status == Status.OK)
                                {
                                    pubsubPendingContext.TryGetValue(p, out var result);
                                    result.Item1 = serializer.ReadKey(ref src);
                                    result.Item2 = serializer.ReadValue(ref src);
                                    functions.SubscribeCallback(ref result.Item1, ref result.Item2, result.Item3);
                                }
                                else if (status == Status.PENDING)
                                {
                                    var result = pubsubQueue.Dequeue();
                                    pubsubPendingContext.Add(p, result);
                                }
                                else
                                {
                                    throw new Exception("Unexpected status of SubscribeKV");
                                }
                                break;
                            }
                        case MessageType.PendingResult:
                            {
                                HandlePending(ref src);
                                break;
                            }
                        default:
                            throw new NotImplementedException();
                    }
                }
            }
            Interlocked.Decrement(ref numPendingBatches);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status ReadStatus(ref byte* dst) => (Status)(*dst++);

        private void HandlePending(ref byte* src)
        {
            Output defaultOutput = default;
            var origMessage = (MessageType)(*src++);
            var p = hrw.ReadPendingSeqNo(ref src);
            switch (origMessage)
            {
                case MessageType.Read:
                    {
                        var status = ReadStatus(ref src);
#if NETSTANDARD2_1
                        readRmwPendingContext.Remove(p, out var result);
#else
                        readRmwPendingContext.TryGetValue(p, out var result);
                        readRmwPendingContext.Remove(p);
#endif
                        if (status == Status.OK)
                        {
                            result.Item3 = serializer.ReadOutput(ref src);
                            functions.ReadCompletionCallback(ref result.Item1, ref result.Item2, ref result.Item3, result.Item4, status);
                        }
                        else
                            functions.ReadCompletionCallback(ref result.Item1, ref result.Item2, ref defaultOutput, result.Item4, status);
                        break;
                    }
                case MessageType.ReadAsync:
                    {
                        var status = ReadStatus(ref src);
#if NETSTANDARD2_1
                        readRmwPendingTcs.Remove(p, out var result);
#else
                        readRmwPendingTcs.TryGetValue(p, out var result);
                        readRmwPendingTcs.Remove(p);
#endif

                        if (status == Status.OK)
                            result.SetResult((status, serializer.ReadOutput(ref src)));
                        else
                            result.SetResult((status, default));
                        break;
                    }
                case MessageType.RMW:
                    {
                        var status = ReadStatus(ref src);
#if NETSTANDARD2_1
                        readRmwPendingContext.Remove(p, out var result);
#else
                        readRmwPendingContext.TryGetValue(p, out var result);
                        readRmwPendingContext.Remove(p);
#endif
                        if (status == Status.OK || status == Status.NOTFOUND)
                        {
                            result.Item3 = serializer.ReadOutput(ref src);
                            functions.ReadCompletionCallback(ref result.Item1, ref result.Item2, ref result.Item3, result.Item4, status);
                        }
                        else
                            functions.RMWCompletionCallback(ref result.Item1, ref result.Item2, ref defaultOutput, result.Item4, status);
                        break;
                    }
                case MessageType.RMWAsync:
                    {
                        var status = ReadStatus(ref src);
#if NETSTANDARD2_1
                        readRmwPendingTcs.Remove(p, out var result);
#else
                        readRmwPendingTcs.TryGetValue(p, out var result);
                        readRmwPendingTcs.Remove(p);
#endif
                        if (status == Status.OK || status == Status.NOTFOUND)
                            result.SetResult((status, serializer.ReadOutput(ref src)));
                        else
                            result.SetResult((status, default));
                        break;
                    }
                case MessageType.SubscribeKV:
                    {
                        var status = ReadStatus(ref src);
                        if (!readRmwPendingContext.TryGetValue(p, out var result))
                        {
                            Debug.WriteLine("Received unexpected subsription key");
                            break;
                        }

                        if (status == Status.OK)
                        {
                            result.Item3 = serializer.ReadOutput(ref src);
                            functions.ReadCompletionCallback(ref result.Item1, ref result.Item2, ref result.Item3, result.Item4, status);
                        }
                        else
                            functions.ReadCompletionCallback(ref result.Item1, ref result.Item2, ref defaultOutput, result.Item4, status);
                        break;
                    }
                default:
                    {
                        throw new NotImplementedException();
                    }
            }
        }

        private Socket GetSendSocket(string address, int port, int millisecondsTimeout = -2)
        {
            var ip = IPAddress.Parse(address);
            var endPoint = new IPEndPoint(ip, port);
            var socket = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = true
            };

            if (millisecondsTimeout != -2)
            {
                IAsyncResult result = socket.BeginConnect(endPoint, null, null);
                result.AsyncWaitHandle.WaitOne(millisecondsTimeout, true);
                if (socket.Connected)
                    socket.EndConnect(result);
                else
                {
                    socket.Close();
                    throw new Exception("Failed to connect server.");
                }
            }
            else
            {
                socket.Connect(endPoint);
            }

            // Ok to create new event args on accept because we assume a connection to be long-running
            var receiveEventArgs = new SocketAsyncEventArgs();
            var bufferSize = BufferSizeUtils.ServerBufferSize(maxSizeSettings);
            receiveEventArgs.SetBuffer(new byte[bufferSize], 0, bufferSize);
            receiveEventArgs.UserToken =
                new ClientNetworkSession<Key, Value, Input, Output, Context, Functions, ParameterSerializer>(socket, this);
            receiveEventArgs.Completed += RecvEventArg_Completed;
            var response = socket.ReceiveAsync(receiveEventArgs);
            Debug.Assert(response);
            return socket;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe Status InternalRead(MessageType messageType, ref Key key, ref Input input, ref Output output, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(!subscriptionSession);

            while (true)
            {
                byte* end = sendObject.obj.bufferPtr + bufferSize;
                byte* curr = offset;
                if (hrw.Write(messageType, ref curr, (int)(end - curr)))
                    if (hrw.Write(serialNo, ref curr, (int)(end - curr)))
                        if (serializer.Write(ref key, ref curr, (int)(end - curr)))
                            if (serializer.Write(ref input, ref curr, (int)(end - curr)))
                            {
                                numMessages++;
                                offset = curr;
                                readrmwQueue.Enqueue((key, input, output, userContext));
                                return Status.PENDING;
                            }
                Flush();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe Status InternalSubscribeKV(MessageType messageType, ref Key key, ref Input input, Context userContext = default, long serialNo = 0)
        {
            subscriptionSession = true;

            while (true)
            {
                byte* end = sendObject.obj.bufferPtr + bufferSize;
                byte* curr = offset;
                if (hrw.Write(messageType, ref curr, (int)(end - curr)))
                    if (serializer.Write(ref key, ref curr, (int)(end - curr)))
                        if (serializer.Write(ref input, ref curr, (int)(end - curr)))
                        {
                            numMessages++;
                            offset = curr;
                            readrmwQueue.Enqueue((key, input, default, userContext));
                            return Status.PENDING;
                        }
                Flush();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe Status InternalPublish(MessageType messageType, ref Key key, ref Value desiredValue, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(!subscriptionSession);

            while (true)
            {
                byte* end = sendObject.obj.bufferPtr + bufferSize;
                byte* curr = offset;
                if (hrw.Write(messageType, ref curr, (int)(end - curr)))
                    if (serializer.Write(ref key, ref curr, (int)(end - curr)))
                        if (serializer.Write(ref desiredValue, ref curr, (int)(end - curr)))
                        {
                            numMessages++;
                            offset = curr;
                            upsertQueue.Enqueue((key, desiredValue, userContext));
                            return Status.PENDING;
                        }
                Flush();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe Status InternalSubscribe(MessageType messageType, ref Key key, Context userContext = default, long serialNo = 0)
        {
            subscriptionSession = true;

            while (true)
            {
                byte* end = sendObject.obj.bufferPtr + bufferSize;
                byte* curr = offset;
                if (hrw.Write(messageType, ref curr, (int)(end - curr)))
                    if (serializer.Write(ref key, ref curr, (int)(end - curr)))
                    {
                        numMessages++;
                        offset = curr;
                        pubsubQueue.Enqueue((key, default, userContext));
                        return Status.PENDING;
                    }
                Flush();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe Status InternalUpsert(MessageType messageType, ref Key key, ref Value desiredValue, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(!subscriptionSession);

            while (true)
            {
                byte* end = sendObject.obj.bufferPtr + bufferSize;
                byte* curr = offset;
                if (hrw.Write(messageType, ref curr, (int)(end - curr)))
                    if (hrw.Write(serialNo, ref curr, (int)(end - curr)))
                        if (serializer.Write(ref key, ref curr, (int)(end - curr)))
                            if (serializer.Write(ref desiredValue, ref curr, (int)(end - curr)))
                            {
                                numMessages++;
                                offset = curr;
                                upsertQueue.Enqueue((key, desiredValue, userContext));
                                return Status.PENDING;
                            }
                Flush();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe Status InternalRMW(MessageType messageType, ref Key key, ref Input input, ref Output output, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(!subscriptionSession);

            while (true)
            {
                byte* end = sendObject.obj.bufferPtr + bufferSize;
                byte* curr = offset;
                if (hrw.Write(messageType, ref curr, (int)(end - curr)))
                    if (hrw.Write(serialNo, ref curr, (int)(end - curr)))
                        if (serializer.Write(ref key, ref curr, (int)(end - curr)))
                            if (serializer.Write(ref input, ref curr, (int)(end - curr)))
                            {
                                numMessages++;
                                offset = curr;
                                readrmwQueue.Enqueue((key, input, output, userContext));
                                return Status.PENDING;
                            }
                Flush();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe Status InternalDelete(MessageType messageType, ref Key key, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(!subscriptionSession);

            while (true)
            {
                byte* end = sendObject.obj.bufferPtr + bufferSize;
                byte* curr = offset;
                if (hrw.Write(messageType, ref curr, (int)(end - curr)))
                    if (hrw.Write(serialNo, ref curr, (int)(end - curr)))
                        if (serializer.Write(ref key, ref curr, (int)(end - curr)))
                        {
                            numMessages++;
                            offset = curr;
                            upsertQueue.Enqueue((key, default, userContext));
                            return Status.PENDING;
                        }
                Flush();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool HandleReceiveCompletion(SocketAsyncEventArgs e)
        {
            var connState = (ClientNetworkSession<Key, Value, Input, Output, Context, Functions, ParameterSerializer>)e.UserToken;
            if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success || disposed)
            {
                connState.socket.Dispose();
                e.Dispose();
                return false;
            }

            connState.AddBytesRead(e.BytesTransferred);
            var newHead = connState.TryConsumeMessages(e.Buffer);
            e.SetBuffer(newHead, e.Buffer.Length - newHead);
            return true;
        }

        private void RecvEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            try
            {
                var connState = (ClientNetworkSession<Key, Value, Input, Output, Context, Functions, ParameterSerializer>)e.UserToken;
                do
                {
                    // No more things to receive
                    if (!HandleReceiveCompletion(e)) break;
                } while (!connState.socket.ReceiveAsync(e));
            }
            // ignore session socket disposed due to client session dispose
            catch (ObjectDisposedException) { }
        }
    }
}