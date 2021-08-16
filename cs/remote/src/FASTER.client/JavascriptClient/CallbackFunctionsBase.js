class CallbackFunctionsBase
{
    constructor() { }

    ReadCompletionCallback(key, output, status) { }

    UpsertCompletionCallback(key, value, status) { }

    DeleteCompletionCallback(key, status) { }

    RMWCompletionCallback(key, output, status) { }

    SubscribeKVCompletionCallback(key, output, status) { }
}