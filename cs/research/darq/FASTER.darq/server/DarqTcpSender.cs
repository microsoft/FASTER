using System.Diagnostics;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using FASTER.common;

namespace FASTER.server
{
    public class DarqTcpSender : TcpNetworkSender
    {
        private SimpleObjectPool<SocketAsyncEventArgs> saeaPool = new(() => new SocketAsyncEventArgs());

        public DarqTcpSender(Socket socket, MaxSizeSettings maxSizeSettings) : base(socket, maxSizeSettings) {}

        public DarqTcpSender(Socket socket, int serverBufferSize) : base(socket, serverBufferSize) {}

        /// <inheritdoc />
        public override void SendResponse(byte[] buffer, int offset, int count, object context)
        {
            Debug.Assert(context is ProducerResponseBuffer);
            var saea = saeaPool.Checkout();
            saea.SetBuffer(buffer, offset, count);
            saea.UserToken = context;
            saea.Completed += SaeaBuffer_Completed;
            
            if (Interlocked.Increment(ref throttleCount) > ThrottleMax)
                throttle.Wait();
            if (!socket.SendAsync(saea))
                SaeaBuffer_Completed(null, saea);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SaeaBuffer_Completed(object sender, SocketAsyncEventArgs e)
        {
            SendCallback(e.UserToken);
            saeaPool.Return(e);
            if (Interlocked.Decrement(ref throttleCount) >= ThrottleMax)
                throttle.Release();
        }
        
        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void SendCallback(object context)
        {
            ((ProducerResponseBuffer) context).Dispose();
        }    
    }
}

