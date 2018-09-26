// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FASTER.core;
using System.IO;

namespace FASTER.test
{

    [TestClass]
    public class ObjectFASTERTests
    {
        private static IManagedFasterKV<MyKey, MyValue, MyInput, MyOutput, MyContext> fht;
        private static IDevice log, objlog;
        
        [ClassInitialize]
        public static void Setup(TestContext t)
        {
            log = FasterFactory.CreateLogDevice("hlog", deleteOnClose: true);
            objlog = FasterFactory.CreateObjectLogDevice("hlog", deleteOnClose: true);

            fht = FasterFactory.Create
                <MyKey, MyValue, MyInput, MyOutput, MyContext, MyFunctions>
                (indexSizeBuckets: 128, logDevice: log, objectLogDevice: objlog, functions: new MyFunctions(), 
                LogMutableFraction: 0.1, LogPageSizeBits: 9, LogTotalSizeBytes: 512*16
                );
            fht.StartSession();
        }

        [ClassCleanup]
        public static void TearDown()
        {
            fht.StopSession();
            fht = null;
            log.Close();
        }



        [TestMethod]
        public void ObjectInMemWriteRead()
        {
            var key1 = new MyKey { key = 9999999 };
            var value = new MyValue { value = 23 };

            MyOutput output = new MyOutput();
            fht.Upsert(key1, value, null, 0);
            fht.Read(key1, null, ref output, null, 0);

            Assert.IsTrue(output.value.value == value.value);
        }

        [TestMethod]
        public void ObjectInMemWriteRead2()
        {
            var key1 = new MyKey { key = 8999998 };
            var input1 = new MyInput { value = 23 };

            fht.RMW(key1, input1, null, 0);

            var key2 = new MyKey { key = 8999999 };
            var input2 = new MyInput { value = 24 };
            fht.RMW(key2, input2, null, 0);

            MyOutput output = new MyOutput();
            fht.Read(key1, null, ref output, null, 0);

            Assert.IsTrue(output.value.value == input1.value);

            fht.Read(key2, null, ref output, null, 0);
            Assert.IsTrue(output.value.value == input2.value);

        }


        [TestMethod]
        public void ObjectDiskWriteRead()
        {
            for (int i = 0; i < 2000; i++)
                fht.Upsert(new MyKey { key = i }, new MyValue { value = i }, default(MyContext), 0);

            MyOutput g1 = new MyOutput();
            var status = fht.Read(new MyKey { key = 23 }, new MyInput(), ref g1, new MyContext(), 0);

            if (status == Status.PENDING)
            {
                fht.CompletePending(true);
            }
            else
            {
                Assert.IsTrue(status == Status.OK);
            }

            Assert.IsTrue(g1.value.value == 23);

            status = fht.Read(new MyKey { key = 99999 }, new MyInput(), ref g1, new MyContext(), 0);

            if (status == Status.PENDING)
            {
                fht.CompletePending(true);
            }
            else
            {
                Assert.IsTrue(status == Status.NOTFOUND);
            }


        }
    }
}
