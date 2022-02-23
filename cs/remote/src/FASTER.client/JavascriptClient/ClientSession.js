class ClientSession {

    constructor(address, port, functions, maxSizeSettings) {
        this.functions = functions;
        this.readrmwPendingContext = {};
        this.pubsubPendingContext = {};
        this.maxSizeSettings  = maxSizeSettings ?? new MaxSizeSettings();
        this.bufferSize = JSUtils.ClientBufferSize(this.maxSizeSettings);
        this.serializer = new ParameterSerializer();
        this.readrmwQueue = new Queue();
        this.pubsubQueue = new Queue();
        this.upsertQueue = new Queue();
        this.sendSocket = new ClientNetworkSession(this, address, port);
        this.intSerializer = new IntSerializer();
        this.offset = 4 + BatchHeader.Size;
        this.numMessages = 0;
        this.reusableBuffer = new ArrayBuffer(this.bufferSize);
        this.numPendingBatches = 0;
    }

    ProcessReplies(view, arrayBuf) {
        var count = view.getUint8(8);
        var arrIdx = 4 + BatchHeader.Size;

        for (var i = 0; i < count; i++) {
            const op = view.getUint8(arrIdx);
            arrIdx++;
            const status = view.getUint8(arrIdx);
            arrIdx++;
            var output = [];

            switch (op) {
                case MessageType.Read:
                    var key = this.readrmwQueue.dequeue();
                    if (status == Status.FOUND) {
                        output = this.serializer.ReadOutput(arrayBuf, arrIdx);
                        arrIdx += output.length + 4;
                        this.functions.ReadCompletionCallback(key, output, status);
                        break;
                    } else if (status == Status.PENDING) {
                        var p = this.intSerializer.deserialize(arrayBuf, arrIdx);
                        arrIdx += 4;
                        readrmwPendingContext[p] = key;
                    } else {
                        this.functions.ReadCompletionCallback(key, output, status);
                    }
                    break;

                case MessageType.Upsert:
                    var keyValue = this.upsertQueue.dequeue();
                    this.functions.UpsertCompletionCallback(keyValue[0], keyValue[1], status);
                    break;

                case MessageType.Delete:
                    var keyValue = this.upsertQueue.dequeue();
                    this.functions.DeleteCompletionCallback(keyValue[0], status);
                    break;

                case MessageType.RMW:
                    var key = this.readrmwQueue.dequeue();
                    if (status == Status.FOUND || status == Status.NOTFOUND) {
                        output = this.serializer.ReadOutput(arrayBuf, arrIdx);
                        arrIdx += output.length + 4;
                        this.functions.RMWCompletionCallback(key, output, status);
                    } else if (status == Status.PENDING) {
                        var p = this.intSerializer.deserialize(arrayBuf, arrIdx);
                        arrIdx += 4;
                        readrmwPendingContext[p] = key;
                    } else {
                        output = [];
                        this.functions.RMWCompletionCallback(key, output, status);
                    }
                    break;

                case MessageType.SubscribeKV:
                    var sid = this.intSerializer.deserialize(arrayBuf, arrIdx);
                    arrIdx += 4;
                    if (status == Status.FOUND || status == Status.NOTFOUND) {
                        var key = this.readrmwPendingContext[sid];
                        output = this.serializer.ReadOutput(arrayBuf, arrIdx);
                        arrIdx += output.length + 4;
                        this.functions.SubscribeKVCompletionCallback(key, output, status);
                    } else if (status == Status.PENDING) {
                        var key = this.readrmwQueue.dequeue();
                        this.readrmwPendingContext[sid] = key;
                    } else {
                        var key = this.readrmwPendingContext[sid];
                        output = [];
                        this.functions.SubscribeKVCompletionCallback(key, output, status);
                    }
                    break;

                case MessageType.Subscribe:
                    var sid = this.intSerializer.deserialize(arrayBuf, arrIdx);
                    arrIdx += 4;
                    if (status == Status.FOUND || status == Status.NOTFOUND) {
                        var key = this.pubsubPendingContext[sid];
                        output = this.serializer.ReadOutput(arrayBuf, arrIdx);
                        arrIdx += output.length + 4;
                        this.functions.SubscribeCompletionCallback(key, output, status);
                    } else if (status == Status.PENDING) {
                        var key = this.pubsubQueue.dequeue();
                        this.pubsubPendingContext[sid] = key;
                    } else {
                        var key = this.pubsubPendingContext[sid];
                        value = []
                        this.functions.SubscribeCompletionCallback(key, value, status);
                    }
                    break;

                case MessageType.HandlePending:
                    HandlePending(view, arrayBuf, arrIdx - 1);
                    break;

                default:
                    alert("Wrong reply received");
            }
        }
        this.numPendingBatches--;
    }

    HandlePending(view, arrayBuf, arrIdx) {
        var output = [];
        var origMessage = view.getUint8(arrIdx++);
        var p = this.intSerializer.deserialize(arrayBuf, arrIdx);
        arrIdx += 4;
        var status = view.getUint8(arrIdx++);

        switch (origMessage) {
            case MessageType.Read:
                var key = this.readrmwPendingContext[p];
                delete this.readrmwPendingContext[p];
                if (status == Status.FOUND) {
                    output = this.serializer.ReadOutput(arrayBuf, arrIdx);
                    arrIdx += output.length + 4;
                }
                this.functions.ReadCompletionCallback(key, output, status);
                break;

            case MessageType.RMW:
                var key = this.readrmwPendingContext[p];
                delete this.readrmwPendingContext[p];
                if (status == Status.FOUND) {
                    output = this.serializer.ReadOutput(arrayBuf, arrIdx);
                    arrIdx += output.length + 4;
                }
                this.functions.RMWCompletionCallback(key, output, status);
                break;

            case MessageType.SubscribeKV:
                var key = this.readrmwPendingContext[p];
                if (status == Status.FOUND) {
                    output = this.serializer.ReadOutput(arrayBuf, arrIdx);
                    arrIdx += output.length + 4;
                }
                this.functions.SubscribeKVCompletionCallback(key, output, status);
                break;

            default:
                alert("Not implemented exception");
        }
    }

    /// <summary>
    /// Flush current buffer of outgoing messages. Does not wait for responses.
    /// </summary>
    Flush() {
        if (this.offset > 4 + BatchHeader.Size) {

            var payloadSize = this.offset;
            this.intSerializer.serialize(this.reusableBuffer, 4, 0);
            this.intSerializer.serialize(this.reusableBuffer, 8, this.numMessages);
            this.intSerializer.serialize(this.reusableBuffer, 0, (payloadSize - 4));
            this.numPendingBatches++;

            var sendingView = new Uint8Array(this.reusableBuffer, 0, payloadSize);
            this.sendSocket.websocket.send(sendingView);
            this.offset = 4 + BatchHeader.Size;
            this.numMessages = 0;
        }
    }

    /// <summary>
    /// Flush current buffer of outgoing messages. Spin-wait for all responses to be received and process them.
    /// </summary>
    CompletePending(wait = true) {
        this.Flush();
    }

    Upsert(key, value) {
        // OP SEQ NUMBER, OP, LEN(KEY), KEY, LEN(VAL), VAL
        var view = new Uint8Array(this.reusableBuffer);

        var arrIdx = this.offset;
        view[arrIdx++] = MessageType.Upsert;

        arrIdx = this.serializer.WriteKVI(this.reusableBuffer, arrIdx, key, key.length);
        arrIdx = this.serializer.WriteKVI(this.reusableBuffer, arrIdx, value, value.length);

        this.upsertQueue.enqueue([key, value]);

        this.offset = arrIdx;
        this.numMessages++;
    }

    Read(key) {
        // OP SEQ NUMBER, OP, LEN(KEY), KEY
        var view = new Uint8Array(this.reusableBuffer);

        var arrIdx = this.offset;
        view[arrIdx++] = MessageType.Read;

        arrIdx = this.serializer.WriteKVI(this.reusableBuffer, arrIdx, key, key.length);

        this.readrmwQueue.enqueue(key);

        this.offset = arrIdx;
        this.numMessages++;
    }

    Delete(key) {
        // OP SEQ NUMBER, OP, LEN(KEY), KEY
        var view = new Uint8Array(this.reusableBuffer);

        var arrIdx = this.offset;
        view[arrIdx++] = MessageType.Delete;

        arrIdx = this.serializer.WriteKVI(this.reusableBuffer, arrIdx, key, key.length);

        var value = [];
        this.upsertQueue.enqueue([key, value]);

        this.offset = arrIdx;
        this.numMessages++;
    }

    RMW(key, input) {
        // OP SEQ NUMBER, OP, LEN(KEY), KEY, LEN(OP_ID + INPUT), OP_ID, INPUT
        var view = new Uint8Array(this.reusableBuffer);

        var arrIdx = this.offset;
        view[arrIdx++] = MessageType.RMW;

        arrIdx = this.serializer.WriteKVI(this.reusableBuffer, arrIdx, key, key.length);
        arrIdx = this.serializer.WriteKVI(this.reusableBuffer, arrIdx, input, input.length);

        this.readrmwQueue.enqueue(key);

        this.offset = arrIdx;
        this.numMessages++;
    }

    SubscribeKV(key) {
        // OP SEQ NUMBER, OP, LEN(KEY), KEY
        var view = new Uint8Array(this.reusableBuffer);

        var arrIdx = this.offset;
        view[arrIdx++] = MessageType.SubscribeKV;

        arrIdx = this.serializer.WriteKVI(this.reusableBuffer, arrIdx, key, key.length);

        this.readrmwQueue.enqueue(key);

        this.offset = arrIdx;
        this.numMessages++;
    }

    Subscribe(key) {
        // OP SEQ NUMBER, OP, LEN(KEY), KEY
        var view = new Uint8Array(this.reusableBuffer);

        var arrIdx = this.offset;
        view[arrIdx++] = MessageType.Subscribe;

        arrIdx = this.serializer.WriteKVI(this.reusableBuffer, arrIdx, key, key.length);

        this.pubsubQueue.enqueue(key);

        this.offset = arrIdx;
        this.numMessages++;
    }
}