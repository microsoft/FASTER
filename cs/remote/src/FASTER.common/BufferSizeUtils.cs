// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.common
{
    /// <summary>
    /// Utility for computing buffer size given settings
    /// </summary>
    public static class BufferSizeUtils

    {
        private const int MaxBatchSize = 1 << 17;

        /// <summary>
        /// Compute required client buffer size
        /// </summary>
        /// <param name="maxSizeSettings">Settings</param>
        /// <returns></returns>
        public static int ClientBufferSize(MaxSizeSettings maxSizeSettings)
        {
            int minSizeUpsert = maxSizeSettings.MaxKeySize + maxSizeSettings.MaxValueSize + 2 + BatchHeader.Size;
            int minSizeReadRmw = maxSizeSettings.MaxKeySize + maxSizeSettings.MaxInputSize + 2 + BatchHeader.Size;

            // leave enough space for double buffering
            int minSize = 2 * (minSizeUpsert < minSizeReadRmw ? minSizeReadRmw : minSizeUpsert) + sizeof(int);

            return MaxBatchSize < minSize ? minSize : MaxBatchSize;
        }

        /// <summary>
        /// Compute required server buffer size
        /// </summary>
        /// <param name="maxSizeSettings">Settings</param>
        /// <returns></returns>
        public static int ServerBufferSize(MaxSizeSettings maxSizeSettings)
        {
            int minSizeRead = maxSizeSettings.MaxOutputSize + 2 + BatchHeader.Size;

            // leave enough space for double buffering
            int minSize = 2 * minSizeRead + sizeof(int);

            return MaxBatchSize < minSize ? minSize : MaxBatchSize;
        }
    }

}
