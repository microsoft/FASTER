class ClientSession {

    constructor(address, port, functions, maxSizeSettings) {
        this.functions = functions;
        this.subscriptionDict = {};
        this.maxSizeSettings  = maxSizeSettings ?? new MaxSizeSettings();
        this.bufferSize = JSUtils.ClientBufferSize(maxSizeSettings);
        this.serializer = new ParameterSerializer();
        this.readrmwQueue = new Queue();
        this.upsertQueue = new Queue();
        this.sendSocket = new ClientNetworkSession(this, address, port);
        this.intSerializer = new IntSerializer();
    }

    ProcessReplies(view, arrayBuf) {
        var opSeqId = this.intSerializer.deserialize(arrayBuf, 0);
        const op = view.getUint8(4);
        const status = view.getUint8(5);

        switch (op) {
            case MessageType.Read:
                if (status == Status.OK) {
                    var key = this.readrmwQueue.dequeue();
                    var output = this.serializer.ReadOutput(arrayBuf, 6);
                    this.functions.ReadCompletionCallback(key, output, status);
                    break;
                } else if (status != Status.PENDING) {
                    var output = [];
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
                if (status == Status.OK || status == Status.NOTFOUND) {
                    var key = this.readrmwQueue.dequeue();
                    var output = this.serializer.ReadOutput(arrayBuf, 6);
                    this.functions.RMWCompletionCallback(key, output, status);
                } else if (status != Status.PENDING) {
                    var key = this.readrmwQueue.dequeue();
                    var output = [];
                    this.functions.RMWCompletionCallback(key, output, status);
                }
                break;

            case MessageType.SubscribeKV:
                var sid = this.intSerializer.deserialize(arrayBuf, 6);
                if (status == Status.OK || status == Status.NOTFOUND) {
                    var key = this.subscriptionDict[sid];
                    var output = this.serializer.ReadOutput(arrayBuf, 10);
                    this.functions.SubscribeKVCompletionCallback(key, output, status);
                } else if (status != Status.PENDING) {
                    var key = this.subscriptionDict[sid];
                    var output = [];
                    this.functions.SubscribeKVCompletionCallback(key, output, status);
                } else {
                    var key = this.readrmwQueue.dequeue();
                    this.subscriptionDict[sid] = key;
                }
                break;

            default:
                alert("Wrong reply received");
        }
    }


    Upsert(key, lenKey, value, lenValue) {
        // OP SEQ NUMBER, OP, LEN(KEY), KEY, LEN(VAL), VAL
        var messageByteArr = new ArrayBuffer(this.bufferSize);
        var view = new Uint8Array(messageByteArr);

        var arrIdx = this.serializer.WriteOpSeqNum(messageByteArr, 0, opSequenceNumber++);
        view[arrIdx++] = MessageType.Upsert;

        var arrIdx = this.serializer.WriteKVI(messageByteArr, arrIdx, key, lenKey);
        var arrIdx = this.serializer.WriteKVI(messageByteArr, arrIdx, value, lenValue);

        var sendingView = new Uint8Array(messageByteArr, 0, arrIdx);

        this.upsertQueue.enqueue([key, value]);

        this.sendSocket.websocket.send(sendingView);

        sendingView = null;
        view = null;
        messageByteArr = null;
    }

    Read(key, lenKey) {
        // OP SEQ NUMBER, OP, LEN(KEY), KEY
        var messageByteArr = new ArrayBuffer(this.bufferSize);
        var view = new Uint8Array(messageByteArr);

        var arrIdx = this.serializer.WriteOpSeqNum(messageByteArr, 0, opSequenceNumber++);
        view[arrIdx++] = MessageType.Read;

        var arrIdx = this.serializer.WriteKVI(messageByteArr, arrIdx, key, lenKey);

        var sendingView = new Uint8Array(messageByteArr, 0, arrIdx);

        this.readrmwQueue.enqueue(key);

        this.sendSocket.websocket.send(sendingView);

        sendingView = null;
        view = null;
        messageByteArr = null;
    }

    Delete(key, lenKey) {
        // OP SEQ NUMBER, OP, LEN(KEY), KEY
        var messageByteArr = new ArrayBuffer(this.bufferSize);
        var view = new Uint8Array(messageByteArr);

        var arrIdx = this.serializer.WriteOpSeqNum(messageByteArr, 0, opSequenceNumber++);
        view[arrIdx++] = MessageType.Delete;

        var arrIdx = this.serializer.WriteKVI(messageByteArr, arrIdx, key, lenKey);

        var sendingView = new Uint8Array(messageByteArr, 0, arrIdx);

        var value = [];
        this.upsertQueue.enqueue([key, value]);

        this.sendSocket.websocket.send(sendingView);

        sendingView = null;
        view = null;
        messageByteArr = null;
    }

    RMW(key, lenKey, input, lenInput) {
        // OP SEQ NUMBER, OP, LEN(KEY), KEY, LEN(OP_ID + INPUT), OP_ID, INPUT
        var messageByteArr = new ArrayBuffer(this.bufferSize);
        var view = new Uint8Array(messageByteArr);

        var arrIdx = this.serializer.WriteOpSeqNum(messageByteArr, 0, opSequenceNumber++);
        view[arrIdx++] = MessageType.RMW;

        var arrIdx = this.serializer.WriteKVI(messageByteArr, arrIdx, key, lenKey);
        var arrIdx = this.serializer.WriteKVI(messageByteArr, arrIdx, input, lenInput);

        var sendingView = new Uint8Array(messageByteArr, 0, arrIdx);

        this.readrmwQueue.enqueue(key);

        this.sendSocket.websocket.send(sendingView);

        sendingView = null;
        view = null;
        messageByteArr = null;
    }

    SubscribeKV(key, lenKey) {
        // OP SEQ NUMBER, OP, LEN(KEY), KEY
        var messageByteArr = new ArrayBuffer(this.bufferSize);
        var view = new Uint8Array(messageByteArr);

        var arrIdx = this.serializer.WriteOpSeqNum(messageByteArr, 0, opSequenceNumber++);
        view[arrIdx++] = MessageType.SubscribeKV;

        var arrIdx = this.serializer.WriteKVI(messageByteArr, arrIdx, key, lenKey);

        var sendingView = new Uint8Array(messageByteArr, 0, arrIdx);

        this.readrmwQueue.enqueue(key);

        this.sendSocket.websocket.send(sendingView);

        sendingView = null;
        view = null;
        messageByteArr = null;
    }
}