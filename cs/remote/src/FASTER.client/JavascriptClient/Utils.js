var MaxBatchSize = 1 << 17;

class MaxSizeSettings
{
    constructor() {
        this.MaxKeySize = 4096;
        this.MaxValueSize = 4096;
        this.MaxInputSize = 4096;
        this.MaxOutputSize = 4096;
    }
}

class JSUtils
{
    constructor() { }

    static ClientBufferSize(maxSizeSettings) {
        var minSizeUpsert = maxSizeSettings.MaxKeySize + maxSizeSettings.MaxValueSize + 2;
        var minSizeReadRmw = maxSizeSettings.MaxKeySize + maxSizeSettings.MaxInputSize + 2;

        // leave enough space for double buffering
        var minSize = 2 * (minSizeUpsert < minSizeReadRmw ? minSizeReadRmw : minSizeUpsert) + 4;

        return MaxBatchSize < minSize ? minSize : MaxBatchSize;
    }

}

const Status = {
    OK: 0,
    NOTFOUND: 1,
    PENDING: 2,
    ERROR: 3
};

const MessageType = {
    Read: 0,
    Upsert: 1,
    RMW: 2,
    Delete: 3,
    ReadAsync: 4,
    UpsertAsync: 5,
    RMWAsync: 6,
    DeleteAsync: 7,
    PendingResult: 8,
    SubscribeKV: 9,
    PSubscribeKV: 10
};
Object.freeze(MessageType);
