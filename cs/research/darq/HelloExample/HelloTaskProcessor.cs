using System.Diagnostics;
using FASTER.client;
using FASTER.common;
using FASTER.darq;
using FASTER.libdpr;

namespace HelloExample;

public class HelloTaskProcessor : IDarqProcessor
{
    // For each processor, maintain a (persistent) count of the number of greetings issued 
    private long count;
    
    private IDarqProcessorClientCapabilities capabilities;
    private WorkerId me;
    private List<WorkerId> workers;
    
    private Random random = new();
    private ThreadLocalObjectPool<byte[]> serializationBufferPool = new(() => new byte[1 << 15]);
    private StepRequest reusableRequest = new(null);

    
    public HelloTaskProcessor(WorkerId me, IDarqClusterInfo clusterInfo)
    {
        this.me = me;
        workers = clusterInfo.GetWorkers().Select(e  => e.Item1).ToList();
    }
    
    public bool ProcessMessage(DarqMessage m)
    {
        switch (m.GetMessageType())
        {
            case DarqMessageType.IN:
            {
                var requestBuilder = new StepRequestBuilder(reusableRequest, me);

                int numGreetingsLeft;
                string name;
                unsafe
                {
                    fixed (byte* b = m.GetMessageBody())
                    {
                        numGreetingsLeft = *(int*)b;
                        // This is a special termination signal
                        if (numGreetingsLeft == -1)
                        {
                            m.Dispose();
                            // Return false to signal that there are no more messages to process and the processing
                            // loop can exit
                            return false;
                        }
                        var size = *(int*) (b + sizeof(int));
                        name = new string((sbyte*)b, 2  * sizeof(int), size);
                    }
                }
                

                string suffix;
                switch (++count % 10)
                {
                    case 1:
                        suffix = "st";
                        break;
                    case 2:
                        suffix = "nd";
                        break;
                    case 3:
                        suffix = "rd";
                        break;
                    default:
                        suffix = "th";
                        break;
                }

                // Mark the input message as consumed
                requestBuilder.MarkMessageConsumed(m.GetLsn());
                // send the updated count to self for recoverability
                requestBuilder.AddSelfMessage(BitConverter.GetBytes(count));

                // Decide if we need to send request to another DARQ
                if (numGreetingsLeft != 0)
                {
                    var serializationBuffer = serializationBufferPool.Checkout();
                    var message = ComposeNextRequest(serializationBuffer, numGreetingsLeft - 1);
                    var nextWorker = workers[random.Next() % workers.Count];

                    requestBuilder.AddOutMessage(nextWorker, message);
                    
                    serializationBufferPool.Return(serializationBuffer);
                }
                else
                {
                    // Send termination sequence to everyone so they exit 
                    foreach (var w in workers)
                        requestBuilder.AddOutMessage(w, BitConverter.GetBytes(-1));
                }

                var v = capabilities.Step(requestBuilder.FinishStep());
                Debug.Assert(v.Result == StepStatus.SUCCESS);
                m.Dispose();
                
                Console.WriteLine($"Greetings, {name}! You are the {count}{suffix} user to be greeted on worker {me.guid}!");
                if (numGreetingsLeft == 0)
                    Console.WriteLine("That's it folks. Bye!");
                
                return true;
            }
            case DarqMessageType.SELF:
                count = BitConverter.ToInt32(m.GetMessageBody());
                m.Dispose();
                return true;
            default:
                throw new NotImplementedException();
        }    
    }

    private Span<byte> ComposeNextRequest(byte[] buffer, int numGreetingsLeft)
    {
        var nextName = Program.namePool[random.Next() % Program.namePool.Length];
                    
        var messageSize = nextName.Length + 2 * sizeof(int);
        Debug.Assert(buffer.Length > messageSize);
        unsafe
        {
            fixed (byte* b = buffer)
            {
                var head = b;
                *(int*) head = numGreetingsLeft;
                head += sizeof(int);
                *(int*)head = nextName.Length;
                head += sizeof(int);
                foreach (var t in nextName)
                    *head++ = (byte)t;
            }
        }

        return new Span<byte>(buffer, 0, messageSize);
    }

    public void OnRestart(IDarqProcessorClientCapabilities capabilities)
    {
        this.capabilities = capabilities;
    }
}