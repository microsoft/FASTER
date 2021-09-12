using System.Net.Sockets;
using FASTER.common;
using FASTER.core;

namespace FASTER.server
{
    internal abstract class FasterKVServerSessionBase<Key, Value, Input, Output, Functions, ParameterSerializer> : FasterKVServerSessionBase<Output>
        where Functions : IFunctions<Key, Value, Input, Output, long>
        where ParameterSerializer : IServerSerializer<Key, Value, Input, Output>
    {
        protected readonly ClientSession<Key, Value, Input, Output, long, ServerKVFunctions<Key, Value, Input, Output, Functions>> session;
        protected readonly ParameterSerializer serializer;

        public FasterKVServerSessionBase(Socket socket, FasterKV<Key, Value> store, Functions functions,
            SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings,
            ParameterSerializer serializer, MaxSizeSettings maxSizeSettings)
            : base(socket, maxSizeSettings)
        {
            session = store.For(new ServerKVFunctions<Key, Value, Input, Output, Functions>(functions, this))
                .NewSession<ServerKVFunctions<Key, Value, Input, Output, Functions>>(sessionVariableLengthStructSettings: sessionVariableLengthStructSettings);
            this.serializer = serializer;
        }

        public override void Dispose()
        {
            session.Dispose();
            base.Dispose();
        }
    }

    internal abstract class FasterKVServerSessionBase<Output> : ServerSessionBase
    {
        public FasterKVServerSessionBase(Socket socket, MaxSizeSettings maxSizeSettings) : base(socket, maxSizeSettings) { }

        public abstract void CompleteRead(ref Output output, long ctx, Status status);
        public abstract void CompleteRMW(ref Output output, long ctx, Status status);
    }
}