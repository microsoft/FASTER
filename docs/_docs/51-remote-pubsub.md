---
title: "Remote FASTER - Publish/Subscribe"
permalink: /docs/pubsub/
excerpt: "Remote FASTER"
last_modified_at: 2021-10-21
toc: false
classes: wide
---

FASTER now supports Publish/Subscribe with remote clients. Clients can now subscribe to keys or prefixes of keys and get all updates to the values of 
the keys made by any other client. For ASCII keys, clients can also use pattern-based subscriptions, with support for all glob-style patterns. 
Following are the options for Publish/Subscribe:

1. **Publish/Subscribe with KV**:
One or multiple clients can subscribe to the updates of a key or pattern stored in FASTER. Whenever there is a call for `Upsert()` or `RMW()` for the 
subscribed key or pattern, all the subscribers for the key/pattern are notified of the change. 

2. **Publish/Subscribe without KV**:
One or multiple clients can subscribe to a key or pattern that is not stored in the FASTER key-value store. Whenever there is a call to `Publish()`
a key or pattern, all the subscribers for the key/pattern are notified of the change. 

The basic approach in order to use Publish/Subscribe (with and without KV) is as follows:

## Creating Subscribe(KV)Broker

You can create a Subscribe(KV)Broker with either fixed-size (blittable struct) Key and Value types or variable-sized (varlen) Key and Value types 
similar to byte arrays. The Subscribe(KV)Broker must be created along with the `FASTERServer`, and passed to the provider. 
`FasterLog` is used by the broker for storing the keys and values until they are forwarded to the subscribers.

The method of creating a SubscribeBroker is as follows:

```cs
var kvBroker = new SubscribeKVBroker<SpanByte, SpanByte, SpanByte, IKeyInputSerializer<SpanByte, SpanByte>>(new SpanByteKeySerializer(), null, true);
var broker = new SubscribeBroker<SpanByte, SpanByte, IKeySerializer<SpanByte>>(new SpanByteKeySerializer(), null, true);
```

The first argument is a `IKeySerializer` used for serializing/deserializing keys for pattern-based subscriptions. Second argument is the location 
of the log directory used for `FasterLog`. The last argument is a boolean, whether the `FasterLog` should start fresh, or should recover from the 
previous state.

## Subscribing to a Key / Pattern from clients:

A `FASTERClient` can subscribe to a key or glob-pattern with the following command:

```cs
clientSession.Subscribe(key); // Used for subscribing to a key that is not stored in FasterKV
clientSession.PSubscribe(pattern); // Used for subscribing to a glob-style pattern that is not stored in FasterKV
clientSession.SubscribeKV(key); // Used for subscribing to a key that is stored in FasterKV
clientSession.PSubscribe(pattern); // Used for subscribing to a glob-style pattern that is stored in FasterKV
```

The clientSession can be used to subscribe to multiple keys or patterns in the same session. Once a key or pattern is subscribed, 
the clientSession cannot accept other commands (such as `Upsert()`, `RMW()`, etc) until all the keys or patterns are unsubscribed.  

## Publishing a key from a client:

a `FASTERClient` can publish a key and value, for pushing the updated value for the key to all its subscribers either synchronously or asynchronously. 
```cs
clientSession.Publish(key, value); // Used for publishing a key and value that is not stored in FasterKV, asynchronously
clientSession.PublishNow(key, value); // Used for publishing a key and value and is not stored in FasterKV, synchronously
``` 
For the case of (P)SubscribeKV, the key and value is automatically pushed to the subscribers on `Upsert()` or `RMW()`.
