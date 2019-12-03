// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace VarLenStructSample
{
    /// <summary>
    /// Callback functions for FASTER operations
    /// </summary>
    public class VarLenFunctions : IFunctions<VarLenType, VarLenType, int[], int[], Empty>
    {
        public void RMWCompletionCallback(ref VarLenType key, ref int[] input, Empty ctx, Status status)
        {
        }

        public void ReadCompletionCallback(ref VarLenType key, ref int[] input, ref int[] output, Empty ctx, Status status)
        {
            if (status != Status.OK)
            {
                Console.WriteLine("Sample1: Error!");
                return;
            }

            for (int i = 0; i < output.Length; i++)
            {
                if (output[i] != output.Length)
                {
                    Console.WriteLine("Sample1: Error!");
                    return;
                }
            }
        }

        public void UpsertCompletionCallback(ref VarLenType key, ref VarLenType output, Empty ctx)
        {
        }

        public void DeleteCompletionCallback(ref VarLenType key, Empty ctx)
        {
        }

        public void CheckpointCompletionCallback(Guid sessionId, long serialNum)
        {
            Debug.WriteLine("Session {0} reports persistence until {1}", sessionId, serialNum);
        }

        // Read functions
        public void SingleReader(ref VarLenType key, ref int[] input, ref VarLenType value, ref int[] dst)
        {
            value.ToIntArray(ref dst);
        }

        public void ConcurrentReader(ref VarLenType key, ref int[] input, ref VarLenType value, ref int[] dst)
        {
            value.ToIntArray(ref dst);
        }

        // Upsert functions
        public void SingleWriter(ref VarLenType key, ref VarLenType src, ref VarLenType dst)
        {
            src.CopyTo(ref dst);
        }

        public bool ConcurrentWriter(ref VarLenType key, ref VarLenType src, ref VarLenType dst)
        {
            src.CopyTo(ref dst);
            return true;
        }

        // RMW functions
        public void InitialUpdater(ref VarLenType key, ref int[] input, ref VarLenType value)
        {
        }

        public bool InPlaceUpdater(ref VarLenType key, ref int[] input, ref VarLenType value)
        {
            return true;
        }

        public void CopyUpdater(ref VarLenType key, ref int[] input, ref VarLenType oldValue, ref VarLenType newValue)
        {
        }
    }
}
