using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using FASTER.core;

namespace FASTER.libdpr.enhanced
{
    internal class PrecomputedSyncResponse
    {
        internal long worldLine;
        internal byte[] serializedResponse;
        internal int recoveryStateEnd, responseEnd;

        internal PrecomputedSyncResponse(ClusterState clusterState)
        {
            worldLine = clusterState.currentWorldLine;
            var serializedSize = sizeof(long) + RespUtil.DictionarySerializedSize(clusterState.worldLineDivergencePoint);
            if (serializedSize > serializedResponse.Length)
                serializedResponse = new byte[Math.Max(2 * serializedResponse.Length, serializedSize)];

            BitConverter.TryWriteBytes(new Span<byte>(serializedResponse, 0, sizeof(long)),
                clusterState.currentWorldLine);
            recoveryStateEnd = RespUtil.SerializeDictionary(clusterState.worldLineDivergencePoint, serializedResponse, sizeof(long));
            responseEnd = recoveryStateEnd;
        }

        internal void UpdateCut(Dictionary<Worker, long> newCut)
        {
            var serializedSize = RespUtil.DictionarySerializedSize(newCut);
            if (serializedSize > serializedResponse.Length - recoveryStateEnd)
            {
                var newBuffer = new byte[Math.Max(2 * serializedResponse.Length, recoveryStateEnd + serializedSize)];
                Array.Copy(serializedResponse, newBuffer, recoveryStateEnd);
                serializedResponse = newBuffer;
            }

            responseEnd = RespUtil.SerializeDictionary(newCut, serializedResponse, recoveryStateEnd);
        }
    }
}