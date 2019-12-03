// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;

namespace FASTER.test
{
    [TestFixture]
    internal class SimpleTests
    {
        [Test]
        public unsafe void AddressInfoTest()
        {
            AddressInfo info;

            AddressInfo.WriteInfo(&info, 44, 55);
            Assert.IsTrue(info.Address == 44);
            Assert.IsTrue(info.Size == 512);

            AddressInfo.WriteInfo(&info, 44, 512);
            Assert.IsTrue(info.Address == 44);
            Assert.IsTrue(info.Size == 512);

            AddressInfo.WriteInfo(&info, 44, 513);
            Assert.IsTrue(info.Address == 44);
            Assert.IsTrue(info.Size == 1024);

            if (sizeof(IntPtr) > 4)
            {
                AddressInfo.WriteInfo(&info, 44, 1L << 20);
                Assert.IsTrue(info.Address == 44);
                Assert.IsTrue(info.Size == 1L << 20);

                AddressInfo.WriteInfo(&info, 44, 511 * (1L << 20));
                Assert.IsTrue(info.Address == 44);
                Assert.IsTrue(info.Size == 511 * (1L << 20));

                AddressInfo.WriteInfo(&info, 44, 512 * (1L << 20));
                Assert.IsTrue(info.Address == 44);
                Assert.IsTrue(info.Size == 512 * (1L << 20));

                AddressInfo.WriteInfo(&info, 44, 555555555L);
                Assert.IsTrue(info.Address == 44);
                Assert.IsTrue(info.Size == (1 + (555555555L / 512)) * 512);

                AddressInfo.WriteInfo(&info, 44, 2 * 555555555L);
                Assert.IsTrue(info.Address == 44);
                Assert.IsTrue(info.Size == (1 + (2 * 555555555L / 1048576)) * 1048576);
            }
        }
    }
}
