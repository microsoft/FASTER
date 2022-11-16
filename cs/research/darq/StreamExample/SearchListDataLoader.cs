using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using FASTER.client;
using FASTER.core;
using FASTER.libdpr;
using Newtonsoft.Json;

namespace SimpleStream.searchlist
{
    public class SearchListDataLoader
    {
        private string filename;
        private IDarqClusterInfo info;
        private List<SearchListJson> parsedJson = new();
        private long startTime;

        public SearchListDataLoader(string filename, IDarqClusterInfo info)
        {
            this.filename = filename;
            this.info = info;
        }

        public int LoadData()
        {
            Console.WriteLine("Started loading json messages from file");
            var loadedData = File.ReadLines(filename).ToList();
            parsedJson = loadedData.Select(JsonConvert.DeserializeObject<SearchListJson>).ToList()!;
            Console.WriteLine($"Loading of {parsedJson.Count} messages complete");
            return parsedJson.Count;
        }

        public void Run(WorkerId destDarq)
        {
            var serializationBuffer = new byte[1 << 20];
            var darqClient = new DarqProducerClient(info);
            startTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
            
            for (var i = 0; i < parsedJson.Count; i++)
            {
                var json = parsedJson[i];
                var currentTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                while (currentTime < startTime + json.Timestamp)
                {
                    darqClient.ForceFlush();
                    // Busy wait to avoid long delays due to thread scheduling
                    currentTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                }

                json.Timestamp += startTime;
                
                // Copy entry to a buffer
                var m = JsonConvert.SerializeObject(json);
                unsafe
                {
                    fixed (char* mHead = m)
                    {
                        fixed (byte* b = serializationBuffer)
                        {
                            *(int*)b = m.Length;
                            for (var head = 0; head < m.Length; head++)
                                b[head + sizeof(int)] = (byte)mHead[head];
                        }
                    }
                }
                var lsn = i;
                darqClient.EnqueueMessageAsync(destDarq,
                    new ReadOnlySpan<byte>(serializationBuffer, 0, sizeof(int) + m.Length),
                    0, lsn, forceFlush: false);
            }
            Console.WriteLine("########## Finished sending messages to DARQ");
        }
    }
}