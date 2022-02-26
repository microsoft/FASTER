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
    Found: 0,
    NotFound: 1,
    IsPending: 2,
    IsFaulted: 3
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
    SubscribeKV: 8,
    PSubscribeKV: 9,
    Subscribe: 10,
    Publish: 11,
    PSubscribe: 12,
    PendingResult: 13,
};
Object.freeze(MessageType);
