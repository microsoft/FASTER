// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Performance.Common
{
    public class Zipf<TKey>
    {
        // Based on "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al., SIGMOD 1994.

        private RandomGenerator rng;
        private int keyCount;
        private double zetaN, alpha, cutoff2, eta;
        private int[] intermediateKeyIndices;
        private bool verbose;

        public TKey[] GenerateOpKeys(TKey[] initKeys, int opCount, double theta, RandomGenerator rng,
                                      bool shuffle, bool verbose = false)
        {
            this.rng = rng;
            this.keyCount = initKeys.Length;
            this.verbose = verbose;

            this.zetaN = Zeta(keyCount, theta);
            this.alpha = 1.0 / (1.0 - theta);
            this.cutoff2 = Math.Pow(0.5, theta);
            var zeta2 = Zeta(2, theta);
            this.eta = (1.0 - Math.Pow(2.0 / keyCount, 1.0 - theta)) / (1.0 - zeta2 / zetaN);

            this.intermediateKeyIndices = new int[initKeys.Length];
            for (var ii = 0; ii < initKeys.Length; ++ii)
                this.intermediateKeyIndices[ii] = ii;
            if (shuffle)
                this.Shuffle(intermediateKeyIndices);

            var opKeys = new TKey[opCount];
            var verboseInterval = opCount / 20;

            for (var ii = 0; ii < opCount; ++ii)
            {
                var keyIndex = this.NextKeyIndex();

                // Reverse to base at log tail (if it's not shuffled).
                opKeys[ii] = initKeys[intermediateKeyIndices[keyCount - keyIndex - 1]];

                if (verbose && ii % verboseInterval == 0 && ii > 0)
                    Console.WriteLine($"Zipf: {ii}");
            }

            return opKeys;
        }

        [Conditional("DEBUG")]
        internal static void PrintHistogram(IEnumerable<int> keyIndicesEnum, bool shuffled)
        {
            //Console.WriteLine($"\r\nopKeys:\r\n{string.Join("\r\n", opKeyIndices.Select(p => p.ToString()))}");
            Console.WriteLine($"\r\nKeys ({(shuffled ? "" : "not ")} shuffled):");
            var histogramSlots = 100;
            var keyIndices = keyIndicesEnum.ToArray();
            var keysPerSlot = keyIndices.Length / histogramSlots;
            var histogram = new double[histogramSlots];
            foreach (var key in keyIndices)
                ++histogram[key / keysPerSlot];
            for (var kk = 0; kk < histogram.Length; ++kk)
                Console.WriteLine($"{kk,10}: {histogram[kk]} ({(histogram[kk] / keyIndices.Length) * 100:0.##}%)");
        }

        private static double Zeta(int count, double theta)
        {
            double zetaN = 0.0;
            for (var ii = 1; ii <= count; ++ii)
                zetaN += 1.0 / Math.Pow((double)ii, theta);
            return zetaN;
        }

        private int NextKeyIndex()
        {
            double u = (double)this.rng.Generate64(long.MaxValue) / long.MaxValue;
            double uz = u * this.zetaN;
            if (uz < 1)
                return 0;
            if (uz < 1 + this.cutoff2)
                return 1;
            return (int)(this.keyCount * Math.Pow(this.eta * u - this.eta + 1, this.alpha));
        }

        private void Shuffle(int[] array)
        {
            var max = array.Length;

            // Walk through the array swapping an element ahead of us with the current element.
            for (long ii = 0; ii < max - 1; ii++)
            {
                // rng.Next's parameter is exclusive, so idx stays within the array bounds.
                var idx = ii + (long)this.rng.Generate64((ulong)(max - ii));
                var temp = array[idx];
                array[idx] = array[ii];
                array[ii] = temp;
            }
        }
    }
}
