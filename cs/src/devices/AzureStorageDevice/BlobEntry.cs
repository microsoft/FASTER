// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using Microsoft.Azure.Storage.Blob;

namespace FASTER.devices
{
    // This class bundles a page blob object with a queue and a counter to ensure 
    // 1) BeginCreate is not called more than once
    // 2) No writes are issued before EndCreate
    // The creator of a BlobEntry is responsible for populating the object with an underlying Page Blob. Any subsequent callers
    // either directly write to the created page blob, or queues the write so the creator can clear it after creation is complete.
    // In-progress creation is denoted by a null value on the underlying page blob
    class BlobEntry
    {
        private CloudPageBlob pageBlob;
        private ConcurrentQueue<Action<CloudPageBlob>> pendingWrites;
        private int waitingCount;

        /// <summary>
        /// Creates a new BlobEntry to hold the given pageBlob. The pageBlob must already be created.
        /// </summary>
        /// <param name="pageBlob"></param>
        public BlobEntry(CloudPageBlob pageBlob)
        {
            this.pageBlob = pageBlob;
            if (pageBlob != null)
            {
                pendingWrites = new ConcurrentQueue<Action<CloudPageBlob>>();
                waitingCount = 0;
            }

        }
        /// <summary>
        /// Creates a new BlobEntry, does not initialize a page blob. Use <see cref="CreateAsync(long, CloudPageBlob)"/>
        /// for actual creation.
        /// </summary>
        public BlobEntry() : this(null)
        {
        }

        /// <summary>
        /// Getter for the underlying <see cref="CloudPageBlob"/>
        /// </summary>
        /// <returns>the underlying <see cref="CloudPageBlob"/>, or null if there is none</returns>
        public CloudPageBlob GetPageBlob()
        {
            return pageBlob;
        }

        /// <summary>
        /// Asynchronously invoke create on the given pageBlob.
        /// </summary>
        /// <param name="size">maximum size of the blob</param>
        /// <param name="pageBlob">The page blob to create</param>
        public void CreateAsync(long size, CloudPageBlob pageBlob)
        {
            Debug.Assert(waitingCount == 0, "Create should be called on blobs that don't already exist and exactly once");
            // Asynchronously create the blob
            pageBlob.BeginCreate(size, ar =>
            {
                try
                {
                    pageBlob.EndCreate(ar);
                }
                catch (Exception e)
                {
                    // TODO(Tianyu): Can't really do better without knowing error behavior
                    Trace.TraceError(e.Message);
                }
                // At this point the blob is fully created. After this line all consequent writers will write immediately. We just
                // need to clear the queue of pending writers.
                this.pageBlob = pageBlob;
                // Take a snapshot of the current waiting count. Exactly this many actions will be cleared.
                // Swapping in -1 will inform any stragglers that we are not taking their actions and prompt them to retry (and call write directly)
                int waitingCountSnapshot = Interlocked.Exchange(ref waitingCount, -1);
                Action<CloudPageBlob> action;
                // Clear actions
                for (int i = 0; i < waitingCountSnapshot; i++)
                {
                    // inserts into the queue may lag behind the creation thread. We have to wait until that happens.
                    // This is so rare, that we are probably okay with a busy wait.
                    while (!pendingWrites.TryDequeue(out action)) { }
                    action(pageBlob);
                }
                // Mark for deallocation for the GC
                pendingWrites = null;
            }, null);
        }

        /// <summary>
        /// Attempts to enqueue an action to be invoked by the creator after creation is done. Should only be invoked when
        /// creation is in-flight. This call is allowed to fail (and return false) if concurrently the creation is complete.
        /// The caller should call the write action directly instead of queueing in this case.
        /// </summary>
        /// <param name="writeAction">The write action to perform</param>
        /// <returns>Whether the action was successfully enqueued</returns>
        public bool TryQueueAction(Action<CloudPageBlob> writeAction)
        {
            int currentCount;
            do
            {
                currentCount = waitingCount;
                // If current count became -1, creation is complete. New queue entries will not be processed and we must call the action ourselves.
                if (currentCount == -1) return false;
            } while (Interlocked.CompareExchange(ref waitingCount, currentCount + 1, currentCount) != currentCount);
            // Enqueue last. The creation thread is obliged to wait until it has processed waitingCount many actions.
            // It is extremely unlikely that we will get scheduled out here anyways.
            pendingWrites.Enqueue(writeAction);
            return true;
        }
    }
}
