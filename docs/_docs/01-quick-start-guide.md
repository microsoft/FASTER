---
title: "Quick-Start Guide"
permalink: /docs/quick-start-guide/
excerpt: "Get started with FASTER"
last_modified_at: 2020-11-08
toc: false
classes: wide
---

<img src="https://raw.githubusercontent.com/microsoft/FASTER/master/docs/assets/images/faster-logo.png" alt="FASTER logo" width="600px" />

## Key Links

* Home Page: [https://aka.ms/FASTER](https://aka.ms/FASTER)
* Source Code: [https://github.com/microsoft/FASTER](https://github.com/microsoft/FASTER)
* C# Samples: [https://github.com/microsoft/FASTER/tree/master/cs/samples](https://github.com/microsoft/FASTER/tree/master/cs/samples)
* C# NuGet binary feed:
  * [Microsoft.FASTER.Core](https://www.nuget.org/packages/Microsoft.FASTER.Core/)
  * [Microsoft.FASTER.Devices.AzureStorage](https://www.nuget.org/packages/Microsoft.FASTER.Devices.AzureStorage/)
* Research papers: [link](/FASTER/docs/td-research-papers/)


Choose links from the navigation menu to learn more and start using FASTER.


## Embedded key-value store sample (without checkpointing)

```cs
public static void Main()
{
  using var log = Devices.CreateLogDevice("hlog.log"); // backing storage device
  using var store = new FasterKV<long, long>(1L << 20, // hash table size (number of 64-byte buckets)
     new LogSettings { LogDevice = log } // log settings (devices, page size, memory size, etc.)
     );

  // Create a session per sequence of interactions with FASTER
  // We use default callback functions with a custom merger: RMW merges input by adding it to value
  using var s = store.NewSession(new SimpleFunctions<long, long>((a, b) => a + b));
  long key = 1, value = 1, input = 10, output = 0;
  
  // Upsert and Read
  s.Upsert(ref key, ref value);
  s.Read(ref key, ref output);
  Debug.Assert(output == value);
  
  // Read-Modify-Write (add input to value)
  s.RMW(ref key, ref input);
  s.RMW(ref key, ref input);
  s.Read(ref key, ref output);
  Debug.Assert(output == value + 20);
  Console.WriteLine("Success!");
}
```
