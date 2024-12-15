// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;
using FASTER.core;

namespace FASTER.test.statemachine
{
    internal static class Extension
    {
        public static ThreadSession<K, V, I, O, C, F>
            CreateThreadSession<K, V, I, O, C, F>
            (this FasterKV<K, V>.ClientSessionBuilder<I,O,C> fht, F f)
            where K : new() where V : new() where F : IFunctions<K, V, I, O, C>
        {
            return new ThreadSession<K, V, I, O, C, F>(fht, f);
        }

        public static LUCThreadSession<K, V, I, O, C, F>
            CreateLUCThreadSession<K, V, I, O, C, F>
            (this FasterKV<K, V>.ClientSessionBuilder<I, O, C> fht, F f)
            where K : new() where V : new() where F : IFunctions<K, V, I, O, C>
        {
            return new LUCThreadSession<K, V, I, O, C, F>(fht, f);
        }
    }

    internal class ThreadSession<K,V,I,O,C,F>
        where K : new() where V : new() where F : IFunctions<K,V,I,O,C>
    {
        readonly FasterKV<K, V>.ClientSessionBuilder<I, O, C> fht;
        ClientSession<K, V, I, O, C, F> s2;
        UnsafeContext<K, V, I, O, C, F> uc2;
        readonly F f;
        readonly AutoResetEvent ev = new(false);
        readonly AsyncQueue<string> q = new();

        public ThreadSession(FasterKV<K, V>.ClientSessionBuilder<I, O, C> fht, F f)
        {
            this.fht = fht;
            this.f = f;
            var ss = new Thread(() => SecondSession());
            ss.Start();
            ev.WaitOne();
        }

        public void Refresh(bool waitComplete = true)
        {
            OtherSession("refresh", waitComplete);
        }

        public void CompleteOp()
        {
            ev.WaitOne();
        }

        public void Dispose()
        {
            OtherSession("dispose");
        }

        private void SecondSession()
        {
            s2 = fht.NewSession(f, null);
            uc2 = s2.UnsafeContext;
            uc2.BeginUnsafe();

            ev.Set();

            while (true)
            {
                var cmd = q.DequeueAsync().Result;
                switch (cmd)
                {
                    case "refresh":
                        uc2.Refresh();
                        ev.Set();
                        break;
                    case "dispose":
                        uc2.EndUnsafe();
                        s2.Dispose();
                        ev.Set();
                        return;
                    default:
                        throw new Exception("Unsupported command");
                }
            }
        }

        private void OtherSession(string command, bool waitComplete = true)
        {
            q.Enqueue(command);
            if (waitComplete) ev.WaitOne();
        }
    }

    internal class LUCThreadSession<K, V, I, O, C, F>
        where K : new() where V : new() where F : IFunctions<K, V, I, O, C>
    {
        readonly FasterKV<K, V>.ClientSessionBuilder<I, O, C> fht;
        ClientSession<K, V, I, O, C, F> session;
        LockableUnsafeContext<K, V, I, O, C, F> luc;
        readonly F f;
        readonly AutoResetEvent ev = new(false);
        readonly AsyncQueue<string> q = new();
        public bool isProtected = false;

        public LUCThreadSession(FasterKV<K, V>.ClientSessionBuilder<I, O, C> fht, F f)
        {
            this.fht = fht;
            this.f = f;
            var ss = new Thread(() => LUCThread());
            ss.Start();
            ev.WaitOne();
        }
        public void Refresh()
        {
            queue("refresh");
        }

        public void Dispose()
        {
            queue("dispose");
        }
        public void DisposeLUC()
        {
            queue("DisposeLUC");
        }
        
        public void getLUC()
        {
            queue("getLUC");
        }
        
        private void LUCThread()
        {
            session = fht.NewSession(f, null);
            ev.Set();

            while (true)
            {
                var cmd = q.DequeueAsync().Result;
                switch (cmd)
                {
                    case "refresh":
                        if (isProtected)
                            luc.Refresh();
                        else
                            session.Refresh();
                        ev.Set();
                        break;
                    case "dispose":
                        if (isProtected)
                        {
                            luc.EndUnsafe();
                        }
                        session.Dispose();
                        ev.Set();
                        return;
                    case "getLUC":
                        luc = session.LockableUnsafeContext;
                        if (session.IsInPreparePhase())
                        {
                            this.isProtected = false;
                        }
                        else
                        {
                            luc.BeginUnsafe();
                            luc.BeginLockable();
                            this.isProtected = true;
                        }
                        ev.Set();
                        break;
                    case "DisposeLUC":
                        luc.EndLockable();
                        luc.EndUnsafe();
                        this.isProtected = false;
                        ev.Set();
                        break;
                    default:
                        throw new Exception("Unsupported command");
                }
            }
        }
        private void queue(string command)
        {
            q.Enqueue(command);
            ev.WaitOne();
        }
    }
}
