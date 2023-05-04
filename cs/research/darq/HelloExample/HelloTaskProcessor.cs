using System.Diagnostics;
using FASTER.client;
using FASTER.common;
using FASTER.core;
using FASTER.libdpr;

namespace HelloExample;

public class HelloTaskProcessor : IDarqProcessor
{
    private long count;
    private IDarqProcessorClientCapabilities capabilities;
    private StepRequest reusableRequest = new(null);
    private WorkerId me;
    private List<WorkerId> workers;
    private Random random = new();
    private ThreadLocalObjectPool<byte[]> serializationBufferPool = new(() => new byte[1 << 15]);
    
    public HelloTaskProcessor(WorkerId me, IDarqClusterInfo clusterInfo)
    {
        this.me = me;
        workers = clusterInfo.GetWorkers().Select(e => e.Item1).ToList();
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

                requestBuilder.AddSelfMessage(BitConverter.GetBytes(count));
                // TODO(Tianyu): Is this correct?
                requestBuilder.ConsumeUntil(m.GetLsn());
                
                if (numGreetingsLeft != 0)
                {
                    var nextName = Program.namePool[random.Next() % Program.namePool.Length];
                    var nextWorker = workers[random.Next() % workers.Count];
                    
                    var serializationBuffer = serializationBufferPool.Checkout();
                    var messageSize = nextName.Length + 2 * sizeof(int);
                    Debug.Assert(serializationBuffer.Length > messageSize);
                    unsafe
                    {
                        fixed (byte* b = serializationBuffer)
                        {
                            var head = b;
                            *(int*) head = numGreetingsLeft - 1;
                            head += sizeof(int);
                            *(int*)head = nextName.Length;
                            head += sizeof(int);
                            foreach (var t in nextName)
                                *head++ = (byte)t;
                        }
                    }

                    requestBuilder.AddOutMessage(nextWorker, new Span<byte>(serializationBuffer, 0, messageSize));
                }

                if (numGreetingsLeft == 0)
                {
                    // Send termination sequence to everyone so they exit 
                    foreach (var w in workers)
                        requestBuilder.AddOutMessage(w, BitConverter.GetBytes(-1));
                }

                capabilities.Step(requestBuilder.FinishStep());
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

    public void OnRestart(IDarqProcessorClientCapabilities capabilities)
    {
        this.capabilities = capabilities;
    }
}