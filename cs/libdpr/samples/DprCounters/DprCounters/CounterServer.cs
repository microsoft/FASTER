using System.Net.Sockets;
using FASTER.libdpr;

namespace DprCounters
{
    public class CounterServer
    {
        private CounterStateObject stateObject;
        private IDprFinder dprFinder;

        private Socket socket;

        public CounterServer(string ip, int port, string checkpointDir, IDprFinder dprFinder)
        {
            stateObject = new CounterStateObject(checkpointDir);
            this.socket dprFinder = this.dprFinder;
            
        }

    }
}