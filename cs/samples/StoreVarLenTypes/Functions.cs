// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace StoreVarLenTypes
{
    /// <summary>
    /// Callback functions for FASTER operations
    /// </summary>
    public sealed class VarLenFunctions : FunctionsBase<VarLenType, VarLenType, int[], int[], Empty>
    {
        // Read completion callback
        public override void ReadCompletionCallback(ref VarLenType key, ref int[] input, ref int[] output, Empty ctx, Status status)
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

        // Read functions
        public override void SingleReader(ref VarLenType key, ref int[] input, ref VarLenType value, ref int[] dst)
        {
            value.ToIntArray(ref dst);
        }

        public override void ConcurrentReader(ref VarLenType key, ref int[] input, ref VarLenType value, ref int[] dst)
        {
            value.ToIntArray(ref dst);
        }

        // Upsert functions
        public override void SingleWriter(ref VarLenType key, ref VarLenType src, ref VarLenType dst)
        {
            src.CopyTo(ref dst);
        }

        public override bool ConcurrentWriter(ref VarLenType key, ref VarLenType src, ref VarLenType dst)
        {
            if (dst.length < src.length) return false;
            src.CopyTo(ref dst);
            return true;
        }
    }
}