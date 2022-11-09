// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.common
{
    /// <summary>
    /// Different types of messages
    /// </summary>
    public enum MessageType : byte
    {
        /// <summary>
        /// A request to read some value in a remote Faster instance
        /// </summary>
        Read,
        /// <summary>
        /// A request to upsert some value in a remote Faster instance
        /// </summary>
        Upsert,
        /// <summary>
        /// A request to rmw on some value in a remote Faster instance
        /// </summary>
        RMW,
        /// <summary>
        /// A request to delete some value in a remote Faster instance
        /// </summary>
        Delete,

        /// <summary>
        /// An async request to read some value in a remote Faster instance
        /// </summary>
        ReadAsync,
        /// <summary>
        /// An async request to upsert some value in a remote Faster instance
        /// </summary>
        UpsertAsync,
        /// <summary>
        /// An async request to rmw on some value in a remote Faster instance
        /// </summary>
        RMWAsync,
        /// <summary>
        /// An async request to delete some value in a remote Faster instance
        /// </summary>
        DeleteAsync,

        /// <summary>
        /// A request to subscribe to some key in a remote Faster instance
        /// </summary>
        SubscribeKV,

        /// <summary>
        /// A request to subscribe to some key prefix in a remote Faster instance
        /// </summary>
        PSubscribeKV,

        /// <summary>
        /// A request to subscribe to some key 
        /// </summary>
        Subscribe,

        /// <summary>
        /// A request to publish to some key, value pair 
        /// </summary>
        Publish,

        /// <summary>
        /// A request to subscribe to some key prefix 
        /// </summary>
        PSubscribe,

        /// <summary>
        /// Pending result
        /// </summary>
        PendingResult,
        
        /// <summary>
        /// DARQ Enqueue
        /// </summary>
        DarqEnqueue,
        
        /// <summary>
        /// DARQ Step
        /// </summary>
        DarqStep,
        
        /// <summary>
        /// DARQ Register Processor
        /// </summary>
        DarqRegisterProcessor,
        
        /// <summary>
        ///  DARQ start push
        /// </summary>
        DarqStartPush,
    }
}