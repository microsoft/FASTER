// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace MemOnlyCache
{
    public class ZipfGenerator
    {
        // Based on "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al., SIGMOD 1994.
        readonly Random rng;
        readonly private int size;
        readonly double theta;

        private double zetaN, alpha, cutoff2, eta;

        public ZipfGenerator(Random rng, int size, double theta = 0.99)
        {
            this.rng = rng;
            this.size = size;
            this.theta = theta;

            this.zetaN = Zeta(size, this.theta);
            this.alpha = 1.0 / (1.0 - this.theta);
            this.cutoff2 = Math.Pow(0.5, this.theta);
            var zeta2 = Zeta(2, this.theta);
            this.eta = (1.0 - Math.Pow(2.0 / size, 1.0 - this.theta)) / (1.0 - zeta2 / zetaN);
        }

        private static double Zeta(int count, double theta)
        {
            double zetaN = 0.0;
            for (var ii = 1; ii <= count; ++ii)
                zetaN += 1.0 / Math.Pow((double)ii, theta);
            return zetaN;
        }

        public int Next()
        {
            double u = (double)this.rng.Next(int.MaxValue) / int.MaxValue;
            double uz = u * this.zetaN;
            if (uz < 1)
                return 0;
            if (uz < 1 + this.cutoff2)
                return 1;
            return (int)(this.size * Math.Pow(this.eta * u - this.eta + 1, this.alpha));
        }
    }
}
