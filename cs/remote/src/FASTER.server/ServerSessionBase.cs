// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Net.Sockets;
using FASTER.common;
using FASTER.core;

namespace FASTER.server
{
    /// <summary>
    /// Abstract base class for server session provider
    /// </summary>
    public abstract class ServerSessionBase : IServerSession
    {
        /// <summary>
        /// Socket
        /// </summary>
        protected readonly Socket socket;

        /// <summary>
        /// Max size settings
        /// </summary>
        protected readonly MaxSizeSettings maxSizeSettings;

        /// <summary>
        /// Response object
        /// </summary>
        protected ReusableObject<SeaaBuffer> responseObject;

        /// <summary>
        /// Bytes read
        /// </summary>
        protected int bytesRead;

        private readonly NetworkSender messageManager;
        private readonly int serverBufferSize;
        

        /// <summary>
        /// Create new instance
        /// </summary>
        /// <param name="socket"></param>
        /// <param name="maxSizeSettings"></param>
        public ServerSessionBase(Socket socket, MaxSizeSettings maxSizeSettings)
        {
            this.socket = socket;
            this.maxSizeSettings = maxSizeSettings;
            serverBufferSize = BufferSizeUtils.ServerBufferSize(maxSizeSettings);
            messageManager = new NetworkSender(serverBufferSize);
            bytesRead = 0;
        }

        /// <inheritdoc />
        public abstract int TryConsumeMessages(byte[] buf);

        /// <inheritdoc />
        public void AddBytesRead(int bytesRead) => this.bytesRead += bytesRead;

        /// <summary>
        /// Get response object
        /// </summary>
        protected void GetResponseObject() { if (responseObject.obj == null) responseObject = messageManager.GetReusableSeaaBuffer(); }

        /// <summary>
        /// Send response
        /// </summary>
        /// <param name="size"></param>
        protected void SendResponse(int size)
        {
            try
            {
                messageManager.Send(socket, responseObject, 0, size);
            }
            catch
            {
                responseObject.Dispose();
            }
        }

        /// <summary>
        /// Send response
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        protected void SendResponse(int offset, int size)
        {
            try
            {
                messageManager.Send(socket, responseObject, offset, size);
            }
            catch
            {
                responseObject.Dispose();
            }
        }

        public unsafe abstract void Publish(int sid, Status status, byte* output, int outputLength, byte* key, bool prefix);


        /// <summary>
        /// Dispose
        /// </summary>
        public virtual void Dispose()
        {
            socket.Dispose();
            if (responseObject.obj != null)
                responseObject.Dispose();
            messageManager.Dispose();
        }
    }
}
