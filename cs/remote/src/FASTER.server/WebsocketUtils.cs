using FASTER.common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace FASTER.server
{
    public class WebsocketUtils
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
        protected SeaaBuffer responseObject;

        /// <summary>
        /// Message manager
        /// </summary>
        protected readonly NetworkSender messageManager;

        private readonly int serverBufferSize;

        internal struct Decoder
        {
            public int msgLen;
            public int maskStart;
            public int dataStart;
        };

        public WebsocketUtils(Socket socket)
        {
            this.socket = socket;
            this.maxSizeSettings = new MaxSizeSettings();
            serverBufferSize = BufferSizeUtils.ServerBufferSize(maxSizeSettings);
            messageManager = new NetworkSender(serverBufferSize);
        }

        public unsafe void UpgradeHTTPToWebSocket(byte[] buf)
        {
            responseObject = messageManager.GetReusableSeaaBuffer();

            byte* d = responseObject.bufferPtr;
            var dend = d + responseObject.buffer.Length;
            byte* dcurr = d; // reserve space for size

            // 1. Obtain the value of the "Sec-WebSocket-Key" request header without any leading or trailing whitespace
            // 2. Concatenate it with "258EAFA5-E914-47DA-95CA-C5AB0DC85B11" (a special GUID specified by RFC 6455)
            // 3. Compute SHA-1 and Base64 hash of the new value
            // 4. Write the hash back as the value of "Sec-WebSocket-Accept" response header in an HTTP response
            string s = Encoding.UTF8.GetString(buf, 0, buf.Length);
            string swk = Regex.Match(s, "Sec-WebSocket-Key: (.*)").Groups[1].Value.Trim();
            string swka = swk + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
            byte[] swkaSha1 = System.Security.Cryptography.SHA1.Create().ComputeHash(Encoding.UTF8.GetBytes(swka));
            string swkaSha1Base64 = Convert.ToBase64String(swkaSha1);

            // HTTP/1.1 defines the sequence CR LF as the end-of-line marker
            byte[] response = Encoding.UTF8.GetBytes(
                "HTTP/1.1 101 Switching Protocols\r\n" +
                "Connection: Upgrade\r\n" +
                "Upgrade: websocket\r\n" +
                "Sec-WebSocket-Accept: " + swkaSha1Base64 + "\r\n\r\n");

            fixed (byte* responsePtr = &response[0])
                Buffer.MemoryCopy(responsePtr, dcurr, response.Length, response.Length);

            dcurr += response.Length;

            try
            {
                messageManager.Send(socket, responseObject, (int)(d - responseObject.bufferPtr), (int)(dcurr - d));
            }
            catch
            {
                messageManager.Return(responseObject);
            }
        }

        public unsafe (byte[], int) DecodeWebsocketHeader(byte[] buf, int offset, int bytesRead)
        {
            //byte* d = responseObject.bufferPtr;
            //var dend = d + responseObject.buffer.Length;
            //byte* dcurr = d; // reserve space for size
            //var bytesAvailable = bytesRead - readHead;
            //var _origReadHead = readHead;
            int msglen = 0;
            byte[] decoded = Array.Empty<byte>();
            //var ptr = recvBufferPtr + readHead;
            var totalMsgLen = 0;
            List<Decoder> decoderInfoList = new();

            var decoderInfo = new Decoder();

            bool fin = (buf[offset] & 0b10000000) != 0,
                mask = (buf[offset + 1] & 0b10000000) != 0; // must be true, "All messages from the client to the server have this bit set"

            int opcode = buf[offset] & 0b00001111; // expecting 1 - text message
            offset++;

            msglen = buf[offset] - 128; // & 0111 1111

            if (msglen < 125)
            {
                offset++;
            }
            else if (msglen == 126)
            {
                msglen = BitConverter.ToUInt16(new byte[] { buf[offset + 2], buf[offset + 1] }, 0);
                offset += 3;
            }
            else if (msglen == 127)
            {
                msglen = (int)BitConverter.ToUInt64(new byte[] { buf[offset + 8], buf[offset + 7], buf[offset + 6], buf[offset + 5], buf[offset + 4], buf[offset + 3], buf[offset + 2], buf[offset + 1] }, 0);
                offset += 9;
            }

            if (msglen == 0)
                Console.WriteLine("msglen == 0");

            decoderInfo.maskStart = offset;
            decoderInfo.msgLen = msglen;
            decoderInfo.dataStart = offset + 4;
            decoderInfoList.Add(decoderInfo);
            totalMsgLen += msglen;
            offset += 4;

            if (fin == false)
            {
                byte[] decodedClientMsgLen = new byte[sizeof(Int32)];
                byte[] clientMsgLenMask = new byte[4] { buf[decoderInfo.maskStart], buf[decoderInfo.maskStart + 1], buf[decoderInfo.maskStart + 2], buf[decoderInfo.maskStart + 3] };
                for (int i = 0; i < sizeof(Int32); ++i)
                    decodedClientMsgLen[i] = (byte)(buf[decoderInfo.dataStart + i] ^ clientMsgLenMask[i % 4]);
                var clientMsgLen = (int)BitConverter.ToInt32(decodedClientMsgLen, 0);
                if (clientMsgLen > bytesRead)
                    return (null, -1);
            }

            var nextBufOffset = offset;

            while (fin == false)
            {
                nextBufOffset += msglen;

                fin = ((buf[nextBufOffset]) & 0b10000000) != 0;

                nextBufOffset++;
                var nextMsgLen = buf[nextBufOffset] - 128; // & 0111 1111

                offset++;
                nextBufOffset++;

                if (nextMsgLen < 125)
                {
                    nextBufOffset++;
                    offset++;
                }
                else if (nextMsgLen == 126)
                {
                    offset += 3;
                    nextMsgLen = BitConverter.ToUInt16(new byte[] { buf[nextBufOffset + 1], buf[nextBufOffset] }, 0);
                    nextBufOffset += 2;
                }
                else if (nextMsgLen == 127)
                {
                    offset += 9;
                    nextMsgLen = (int)BitConverter.ToUInt64(new byte[] { buf[nextBufOffset + 7], buf[nextBufOffset + 6], buf[nextBufOffset + 5], buf[nextBufOffset + 4], buf[nextBufOffset + 3], buf[nextBufOffset + 2], buf[nextBufOffset + 1], buf[nextBufOffset] }, 0);
                    nextBufOffset += 8;
                }

                var nextDecoderInfo = new Decoder();
                nextDecoderInfo.msgLen = nextMsgLen;
                nextDecoderInfo.maskStart = nextBufOffset;
                nextDecoderInfo.dataStart = nextBufOffset + 4;
                decoderInfoList.Add(nextDecoderInfo);
                totalMsgLen += nextMsgLen;
                offset += 4;
            }

            //bool completeWSCommand = true;

            var decodedIndex = 0;
            decoded = new byte[totalMsgLen];
            for (int decoderListIdx = 0; decoderListIdx < decoderInfoList.Count; decoderListIdx++)
            {
                {
                    var decoderInfoElem = decoderInfoList[decoderListIdx];
                    byte[] masks = new byte[4] { buf[decoderInfoElem.maskStart], buf[decoderInfoElem.maskStart + 1], buf[decoderInfoElem.maskStart + 2], buf[decoderInfoElem.maskStart + 3] };

                    for (int i = 0; i < decoderInfoElem.msgLen; ++i)
                        decoded[decodedIndex++] = (byte)(buf[decoderInfoElem.dataStart + i] ^ masks[i % 4]);
                }
            }

            offset += totalMsgLen;            
            return (decoded, offset);
        }
    }
}
