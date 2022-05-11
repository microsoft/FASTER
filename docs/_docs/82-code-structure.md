---
title: "Code Structure"
permalink: /docs/code-structure/
excerpt: "Code Structure"
last_modified_at: 2020-11-08
toc: false
classes: wide
---

Check out https://github.com/microsoft/FASTER. More details will be added in future.

## Lifecycle of a Read operation for disk data

Here is the sequence of steps from FASTER side, during a Read operation for disk data:
1.	User issues `Read` operation
2.	We create a `PendingContext` object for the read
3.	The `HandleOperationStatus()` call adds `PendingContext` object to `ioPendingRequests` dictionary
4.	Create `AsyncIOContext`  which has pointer to a `readyResponses` queue (also called `callbackQueue`), for that session
5.	Issue IO with `AsyncIOContext` as param
6.	…
7.	IO callback accesses `readyResponses`queue, and enqueues the result in there (result is just the `AsyncIOContext` itself)
8.	…
9.	User calls `CompletePending` on session, which does this:
    * Dequeue entry from `readyResponses` queue
    * Remove item from `ioPendingRequests` dictionary
    * Finally return result to user via `ReadCompletionCallback`

