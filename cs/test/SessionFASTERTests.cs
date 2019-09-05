// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using FASTER.core;
using System.IO;
using NUnit.Framework;

namespace FASTER.test.async
{

    [TestFixture]
    internal class SessionFASTERTests
    {
        private FasterKV<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions> fht;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog1.log", deleteOnClose: true);
            fht = new FasterKV<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions>
                (128, new Functions(), new LogSettings { LogDevice = log, MemorySizeBits = 29 });
        }

        [TearDown]
        public void TearDown()
        {
            fht.Dispose();
            fht = null;
            log.Close();
        }



        [Test]
        public void SessionTest1()
        {
            using (var session = fht.StartClientSession())
            {
                InputStruct input = default(InputStruct);
                OutputStruct output = default(OutputStruct);

                var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
                var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

                session.Upsert(ref key1, ref value, Empty.Default, 0);
                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);

                if (status == Status.PENDING)
                {
                    session.TryCompletePending(true);
                }
                else
                {
                    Assert.IsTrue(status == Status.OK);
                }

                Assert.IsTrue(output.value.vfield1 == value.vfield1);
                Assert.IsTrue(output.value.vfield2 == value.vfield2);
            }
        }


        [Test]
        public void SessionTest2()
        {
            using (var session1 = fht.StartClientSession())
            using (var session2 = fht.StartClientSession())
            {
                InputStruct input = default(InputStruct);
                OutputStruct output = default(OutputStruct);

                var key1 = new KeyStruct { kfield1 = 14, kfield2 = 15 };
                var value1 = new ValueStruct { vfield1 = 24, vfield2 = 25 };
                var key2 = new KeyStruct { kfield1 = 15, kfield2 = 16 };
                var value2 = new ValueStruct { vfield1 = 25, vfield2 = 26 };

                session1.Upsert(ref key1, ref value1, Empty.Default, 0);
                session2.Upsert(ref key2, ref value2, Empty.Default, 0);

                var status = session1.Read(ref key1, ref input, ref output, Empty.Default, 0);

                if (status == Status.PENDING)
                {
                    session1.TryCompletePending(true);
                }
                else
                {
                    Assert.IsTrue(status == Status.OK);
                }

                Assert.IsTrue(output.value.vfield1 == value1.vfield1);
                Assert.IsTrue(output.value.vfield2 == value1.vfield2);

                status = session2.Read(ref key2, ref input, ref output, Empty.Default, 0);

                if (status == Status.PENDING)
                {
                    session2.TryCompletePending(true);
                }
                else
                {
                    Assert.IsTrue(status == Status.OK);
                }

                Assert.IsTrue(output.value.vfield1 == value2.vfield1);
                Assert.IsTrue(output.value.vfield2 == value2.vfield2);
            }
        }

        [Test]
        public void SessionTest3()
        {
            using (var session = fht.StartClientSession())
            {
                Task.CompletedTask.ContinueWith((t) =>
                {
                    InputStruct input = default(InputStruct);
                    OutputStruct output = default(OutputStruct);

                    var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
                    var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

                    session.Upsert(ref key1, ref value, Empty.Default, 0);
                    var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);

                    if (status == Status.PENDING)
                    {
                        session.TryCompletePending(true);
                    }
                    else
                    {
                        Assert.IsTrue(status == Status.OK);
                    }

                    Assert.IsTrue(output.value.vfield1 == value.vfield1);
                    Assert.IsTrue(output.value.vfield2 == value.vfield2);
                }).Wait();
            }
        }

        [Test]
        public void SessionTest4()
        {
            using (var session1 = fht.StartClientSession())
            using (var session2 = fht.StartClientSession())
            {
                var t1 = Task.CompletedTask.ContinueWith((t) =>
                {
                    InputStruct input = default(InputStruct);
                    OutputStruct output = default(OutputStruct);

                    var key1 = new KeyStruct { kfield1 = 14, kfield2 = 15 };
                    var value1 = new ValueStruct { vfield1 = 24, vfield2 = 25 };

                    session1.Upsert(ref key1, ref value1, Empty.Default, 0);
                    var status = session1.Read(ref key1, ref input, ref output, Empty.Default, 0);

                    if (status == Status.PENDING)
                    {
                        session1.TryCompletePending(true);
                    }
                    else
                    {
                        Assert.IsTrue(status == Status.OK);
                    }

                    Assert.IsTrue(output.value.vfield1 == value1.vfield1);
                    Assert.IsTrue(output.value.vfield2 == value1.vfield2);
                });

                var t2 = Task.CompletedTask.ContinueWith((t) =>
                {
                    InputStruct input = default(InputStruct);
                    OutputStruct output = default(OutputStruct);

                    var key2 = new KeyStruct { kfield1 = 15, kfield2 = 16 };
                    var value2 = new ValueStruct { vfield1 = 25, vfield2 = 26 };

                    session2.Upsert(ref key2, ref value2, Empty.Default, 0);

                    var status = session2.Read(ref key2, ref input, ref output, Empty.Default, 0);

                    if (status == Status.PENDING)
                    {
                        session2.TryCompletePending(true);
                    }
                    else
                    {
                        Assert.IsTrue(status == Status.OK);
                    }

                    Assert.IsTrue(output.value.vfield1 == value2.vfield1);
                    Assert.IsTrue(output.value.vfield2 == value2.vfield2);
                });

                t1.Wait();
                t2.Wait();
            }
        }

        [Test]
        public void SessionTest5()
        {
            var session = fht.StartClientSession();
            var id = session.ID;

            InputStruct input = default(InputStruct);
            OutputStruct output = default(OutputStruct);

            var key1 = new KeyStruct { kfield1 = 16, kfield2 = 17 };
            var value1 = new ValueStruct { vfield1 = 26, vfield2 = 27 };

            session.Upsert(ref key1, ref value1, Empty.Default, 0);
            var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);

            if (status == Status.PENDING)
            {
                session.TryCompletePending(true);
            }
            else
            {
                Assert.IsTrue(status == Status.OK);
            }

            Assert.IsTrue(output.value.vfield1 == value1.vfield1);
            Assert.IsTrue(output.value.vfield2 == value1.vfield2);

            session.Dispose();

            session = fht.StartClientSession();

            var key2 = new KeyStruct { kfield1 = 17, kfield2 = 18 };
            var value2 = new ValueStruct { vfield1 = 27, vfield2 = 28 };

            session.Upsert(ref key2, ref value2, Empty.Default, 0);

            status = session.Read(ref key2, ref input, ref output, Empty.Default, 0);

            if (status == Status.PENDING)
            {
                session.TryCompletePending(true);
            }
            else
            {
                Assert.IsTrue(status == Status.OK);
            }

            status = session.Read(ref key2, ref input, ref output, Empty.Default, 0);

            if (status == Status.PENDING)
            {
                session.TryCompletePending(true);
            }
            else
            {
                Assert.IsTrue(status == Status.OK);
            }

            Assert.IsTrue(output.value.vfield1 == value2.vfield1);
            Assert.IsTrue(output.value.vfield2 == value2.vfield2);

            session.Dispose();
        }
    }
}
