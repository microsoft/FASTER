using System.Diagnostics;
using FASTER.client;
using FASTER.libdpr;

namespace HelloExample;

public class HelloTaskProcessor : IDarqProcessor
{
    private long count;
    private IDarqProcessorClientCapabilities capabilities;
    private StepRequest reusableRequest;
    private WorkerId me;
    private List<WorkerId> workers;
    private Random random = new();

    private static string[] namePool = {
        "Alex",
        "Avery",
        "Brennan",
        "Carson",
        "Dakota",
        "Eli",
        "Elliot",
        "Emery",
        "Finley",
        "Harley",
        "Hayden",
        "Jesse",
        "Jordan",
        "Kai",
        "Morgan",
        "Parker",
        "Reese",
        "Riley",
        "Rowan",
        "Sage"
    };

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
                requestBuilder.MarkMessageConsumed(m.GetLsn());

                var message = m.GetMessageBodyAsString();
                var delimited = message.Split(",");
                Debug.Assert(delimited.Length == 2);
                var name = delimited[0];
                var numGreetingsLeft = int.Parse(delimited[1]);

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
                // Implicitly consume the previous self message
                requestBuilder.StartCheckpoint(m.GetLsn());

                if (numGreetingsLeft != 0)
                {
                    requestBuilder.AddOutMessage(workers[random.Next() % workers.Count],
                        $"{namePool[random.Next() % namePool.Length]}, {numGreetingsLeft - 1}");
                }
                
                capabilities.Step(requestBuilder.FinishStep());
                m.Dispose();
                
                Console.WriteLine($"Greetings, {name}! You are the {count}{suffix} user to be greeted!");
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