using System.Net.Sockets;
using FASTER.common;
using FASTER.core;

namespace FASTER.server
{
    internal abstract class FasterKVServerSessionBase<Key, Value, Input, Output, Functions, ParameterSerializer> : ServerSessionBase
        where Functions : IFunctions<Key, Value, Input, Output, long>
        where ParameterSerializer : IServerSerializer<Key, Value, Input, Output>
    {
        protected readonly ClientSession<Key, Value, Input, Output, long, ServerKVFunctions<Key, Value, Input, Output, Functions, ParameterSerializer>> session;
        protected readonly ParameterSerializer serializer;

        public FasterKVServerSessionBase(Socket socket, FasterKV<Key, Value> store, Functions functions,
            ParameterSerializer serializer, MaxSizeSettings maxSizeSettings)
            : base(socket, maxSizeSettings)
        {
            session = store.For(new ServerKVFunctions<Key, Value, Input, Output, Functions, ParameterSerializer>(functions, this))
                .NewSession<ServerKVFunctions<Key, Value, Input, Output, Functions, ParameterSerializer>>();
            this.serializer = serializer;
        }
        
        public abstract void CompleteRead(ref Output output, long ctx, Status status);
        public abstract void CompleteRMW(long ctx, Status status);


        public override void Dispose()
        {
            session.Dispose();
            base.Dispose();
        }
    }
}