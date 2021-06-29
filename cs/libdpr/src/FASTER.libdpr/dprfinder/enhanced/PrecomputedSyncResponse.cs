using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using FASTER.core;

namespace FASTER.libdpr.enhanced
{
    public class PrecomputedSyncResponse
    {
        private ReaderWriterLockSlim rwLatch;
        private byte[] serializedResponse;
        private int recoveryStateEnd, responseEnd;

        public ReadOnlySpan<byte> ReadResponseProtected()
        {
            rwLatch.EnterReadLock();
            return new ReadOnlySpan<byte>(serializedResponse, 0, responseEnd);
        }

        public void ExitProtection()
        {
            rwLatch.ExitReadLock();
        }

        public void UpdateCut(Dictionary<Worker, long> newCut)
        {
            rwLatch.EnterWriteLock();
            var serializedSize = RespUtil.DictionarySerializedSize(newCut);
            if (serializedSize > serializedResponse.Length - recoveryStateEnd)
            {
                var newBuffer = new byte[Math.Max(2 * serializedResponse.Length, recoveryStateEnd + serializedSize)];
                Array.Copy(serializedResponse, newBuffer, recoveryStateEnd);
                serializedResponse = newBuffer;
            }

            responseEnd = RespUtil.SerializeDictionary(newCut, serializedResponse, recoveryStateEnd);
            Debug.Assert(responseEnd != 0);
            rwLatch.ExitWriteLock();
        }

        public void UpdateClusterState(PersistentState persistentState, Dictionary<Worker, long> cut)
        {
            rwLatch.EnterWriteLock();
            var serializedSize = sizeof(long) + RespUtil.DictionarySerializedSize(persistentState.worldLineDivergencePoint) +
                            RespUtil.DictionarySerializedSize(cut);
            if (serializedSize > serializedResponse.Length)
                serializedResponse = new byte[Math.Max(2 * serializedResponse.Length, serializedSize)];

            BitConverter.TryWriteBytes(new Span<byte>(serializedResponse, 0, sizeof(long)),
                persistentState.currentWorldLine);
            recoveryStateEnd = RespUtil.SerializeDictionary(persistentState.worldLineDivergencePoint, serializedResponse, sizeof(long));
            responseEnd = RespUtil.SerializeDictionary(cut, serializedResponse, recoveryStateEnd);
            rwLatch.ExitWriteLock();
        }
    }
}