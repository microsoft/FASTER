// using System;
// using System.Collections.Generic;
// using System.Diagnostics;
// using System.Runtime.CompilerServices;
//
// namespace FASTER.libdpr
// {
//     public class TestClientObject
//     {
//         public DprClientSession session;
//         private int seqCounter, id;
//         private Dictionary<Worker, TestStateStore> stateStores;
//         private SimpleObjectPool<byte[]> bufs;
//         private Dictionary<int, byte[]> responses;
//         
//         public TestClientObject(DprClientSession session, Dictionary<Worker, TestStateStore> stateStores, int id)
//         {
//             this.session = session;
//             seqCounter = 0;
//             this.id = id;
//             this.stateStores = stateStores;
//             bufs = new SimpleObjectPool<byte[]>(() => new byte[1 << 15]);
//             responses = new Dictionary<int, byte[]>();
//         }
//
//         public int IssueNewOp(int workerId)
//         {
//             var buf = bufs.Checkout();
//             var worker =  new Worker(workerId);
//             var bytes =  session.IssueBatch();
//             var op = ValueTuple.Create(id, seqCounter++);
//             stateStores[worker].Process(bytes, new Span<byte>(buf), op);
//             responses.Add(op.Item2, buf);
//             return op.Item2;
//         }
//
//         public long ResolveOp(int op)
//         {
//             responses.Remove(op, out var buf);
//             Debug.Assert(buf != null);
//             session.ResolveBatch(new ReadOnlySpan<byte>(buf), out var result);
//             bufs.Return(buf);
//             return result[0];
//         }
//     }
// }