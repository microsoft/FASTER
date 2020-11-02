---
layout: splash
permalink: /
hidden: true
header:
  overlay_color: "#5e616c"
  overlay_image: /assets/images/faster-banner.png
  actions:
    - label: "Get Started"
      url: "/docs/quick-start-guide/"
excerpt: >
  A fast concurrent persistent key-value store and log, in C# and C++.<br />
  <small><a href="https://github.com/microsoft/FASTER/releases/tag/v1.7.4">Latest release v1.7.4</a></small>
features:
  - image_path: /assets/images/faster-feature-1.png
    alt: "feature1"
    title: "Feature 1"
    excerpt: "Feature 1 excerpt"
  - image_path: /assets/images/faster-feature-2.png
    alt: "feature2"
    title: "Feature 2"
    excerpt: "Feature 1 excerpt"
  - image_path: /assets/images/faster-feature-3.png
    alt: "feature3"
    title: "Feature 3"
    excerpt: "Feature 3 excerpt"
---

Two paras on FASTER here

{% include feature_row_small id="features" %}

# Embedded key-value store sample in C#

```cs
public static void Main()
{
  using var log = Devices.CreateLogDevice("hlog.log"); // backing storage device
  using var store = new FasterKV<long, long>(1L << 20, // hash table size (number of 64-byte buckets)
     new LogSettings { LogDevice = log } // log settings
     );

  // Create a session per sequence of interactions with FASTER
  using var s = store.NewSession(new SimpleFunctions<long, long>());
  long key = 1, value = 1, input = 10, output = 0;
  
  // Upsert and Read
  s.Upsert(ref key, ref value);
  s.Read(ref key, ref output);
  Debug.Assert(output == value);
  
  // Read-Modify-Write
  s.RMW(ref key, ref input);
  s.RMW(ref key, ref input);
  s.Read(ref key, ref output);
  Debug.Assert(output == value + 20);
}
```

