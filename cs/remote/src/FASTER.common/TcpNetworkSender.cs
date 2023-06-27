// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.common
{
    /// <summary>
    /// TCP network sender
    /// </summary>
    public class TcpNetworkSender : NetworkSenderBase
    {
        /// <summary>
        /// Socket
        /// </summary>
        protected readonly Socket socket;

        /// <summary>
        /// Response object
        /// </summary>
        protected SeaaBuffer responseObject;

        /// <summary>
        /// Reusable SeaaBuffer
        /// </summary>
        readonly SimpleObjectPool<SeaaBuffer> reusableSeaaBuffer;

        /// <summary>
        /// Throttle
        /// </summary>
        readonly protected SemaphoreSlim throttle = new(0);

        /// <summary>
        /// Count of sends for throttling
        /// </summary>
        protected int throttleCount;

        /// <summary>
        /// Max concurrent sends (per session) for throttling
        /// </summary>
        protected const int ThrottleMax = 8;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="socket"></param>
        /// <param name="maxSizeSettings"></param>
        public TcpNetworkSender(
            Socket socket,
            MaxSizeSettings maxSizeSettings)
            : base(maxSizeSettings)
        {
            this.socket = socket;
            this.reusableSeaaBuffer = new SimpleObjectPool<SeaaBuffer>(() => new SeaaBuffer(SeaaBuffer_Completed, this.serverBufferSize));
            this.responseObject = null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="socket"></param>
        /// <param name="serverBufferSize"></param>
        public TcpNetworkSender(
            Socket socket,
            int serverBufferSize)
            : base(serverBufferSize)
        {
            this.socket = socket;
            this.reusableSeaaBuffer = new SimpleObjectPool<SeaaBuffer>(() => new SeaaBuffer(SeaaBuffer_Completed, this.serverBufferSize));
            this.responseObject = null;
        }


        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void GetResponseObject()
        {
            if (responseObject == null)
                this.responseObject = reusableSeaaBuffer.Checkout();
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void ReturnResponseObject()
        {
            reusableSeaaBuffer.Return(responseObject);
            responseObject = null;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override unsafe byte* GetResponseObjectHead()
        {
            if (responseObject != null)
                return responseObject.bufferPtr;
            return base.GetResponseObjectHead();
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override unsafe byte* GetResponseObjectTail()
        {
            if (responseObject != null)
                return responseObject.bufferPtr + responseObject.buffer.Length;
            return base.GetResponseObjectHead();
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool SendResponse(int offset, int size)
        {
            var _r = responseObject;
            if (_r == null) return false;
            responseObject = null;
            try
            {
                Send(socket, _r, offset, size);
            }
            catch
            {
                reusableSeaaBuffer.Return(_r);
                if (Interlocked.Decrement(ref throttleCount) >= ThrottleMax)
                    throttle.Release();
                // Rethrow exception as session is not usable
                throw;
            }
            return true;
        }

        /// <inheritdoc />
        public override void SendResponse(byte[] buffer, int offset, int count, object context)
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc />
        public override void SendCallback(object context)
        {
            throw new System.NotImplementedException();
        }

        /// <inheritdoc />
        public override void Dispose() => Dispose(false);

        /// <inheritdoc />
        public override void Dispose(bool waitForSendCompletion)
        {
            if (!waitForSendCompletion)
                socket.Dispose();

            var _r = responseObject;
            if (_r != null)
                reusableSeaaBuffer.Return(_r);
            reusableSeaaBuffer.Dispose();
            throttle.Dispose();
            if (waitForSendCompletion)
                socket.Dispose();
        }

        /// <inheritdoc />
        public override void Throttle()
        {
            // Short circuit for common case of no network overload
            if (throttleCount < ThrottleMax) return;

            // We are throttling, so wait for throttle to be released by some ongoing sender
            if (Interlocked.Increment(ref throttleCount) > ThrottleMax)
                throttle.Wait();

            // Release throttle, since we used up one slot
            if (Interlocked.Decrement(ref throttleCount) >= ThrottleMax)
                throttle.Release();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void Send(Socket socket, SeaaBuffer sendObject, int offset, int size)
        {
            if (Interlocked.Increment(ref throttleCount) > ThrottleMax)
                throttle.Wait();

            // Reset send buffer
            sendObject.socketEventAsyncArgs.SetBuffer(offset, size);
            // Set user context to reusable object handle for disposal when send is done
            sendObject.socketEventAsyncArgs.UserToken = sendObject;
            if (!socket.SendAsync(sendObject.socketEventAsyncArgs))
                SeaaBuffer_Completed(null, sendObject.socketEventAsyncArgs);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SeaaBuffer_Completed(object sender, SocketAsyncEventArgs e)
        {
           reusableSeaaBuffer.Return((SeaaBuffer)e.UserToken);
           if (Interlocked.Decrement(ref throttleCount) >= ThrottleMax)
                throttle.Release();
        }
    }
}
