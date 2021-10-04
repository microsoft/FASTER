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
        [Category("FasterKV")]
        public unsafe void AddressInfoTest()
        {
            AddressInfo info;

            AddressInfo.WriteInfo(&info, 44, 55);
            Assert.AreEqual(44, info.Address);
            Assert.AreEqual(512, info.Size);

            AddressInfo.WriteInfo(&info, 44, 512);
            Assert.AreEqual(44, info.Address);
            Assert.AreEqual(512, info.Size);

            AddressInfo.WriteInfo(&info, 44, 513);
            Assert.AreEqual(44, info.Address);
            Assert.AreEqual(1024, info.Size);

            if (sizeof(IntPtr) > 4)
            {
                AddressInfo.WriteInfo(&info, 44, 1L << 20);
                Assert.AreEqual(44, info.Address);
                Assert.AreEqual(1L << 20, info.Size);

                AddressInfo.WriteInfo(&info, 44, 511 * (1L << 20));
                Assert.AreEqual(44, info.Address);
                Assert.AreEqual(511 * (1L << 20), info.Size);

                AddressInfo.WriteInfo(&info, 44, 512 * (1L << 20));
                Assert.AreEqual(44, info.Address);
                Assert.AreEqual(512 * (1L << 20), info.Size);

                AddressInfo.WriteInfo(&info, 44, 555555555L);
                Assert.AreEqual(44, info.Address);
                Assert.AreEqual((1 + (555555555L / 512)) * 512, info.Size);

                AddressInfo.WriteInfo(&info, 44, 2 * 555555555L);
                Assert.AreEqual(44, info.Address);
                Assert.AreEqual((1 + (2 * 555555555L / 1048576)) * 1048576, info.Size);
            }
        }
    }
}
