// JavaScript source code

function writeToScreen(message) {
    output.insertAdjacentHTML("afterbegin", "<p>" + message + "</p>");
}

class FASTERFunctions extends CallbackFunctionsBase {

    constructor(client) {
        super();
    }

    ReadCompletionCallback(keyBytes, outputBytes, status) {
        if (status == Status.OK) {
            var output = deserialize(outputBytes, 0, outputBytes.length);
            writeToScreen("<span> value: " + output + " </span>");
        }
    }

    UpsertCompletionCallback(keyBytes, valueBytes, status) {
        if (status == Status.OK) {
            writeToScreen("<span> PUT OK </span>");
        }
    }

    DeleteCompletionCallback(keyBytes, status) { }

    RMWCompletionCallback(keyBytes, outputBytes, status) { }

    SubscribeKVCompletionCallback(keyBytes, outputBytes, status)
    {
        if (status == Status.OK) {
            var key = deserialize(keyBytes, 0, keyBytes.length);
            var output = deserialize(outputBytes, 0, outputBytes.length);
            writeToScreen("<span> subscribed key: " + key + " value: " + output + " </span>");
        }
    }

    SubscribeCompletionCallback(keyBytes, valueBytes, status)
    {
        if (status == Status.OK) {
            var key = deserialize(keyBytes, 0, keyBytes.length);
            var value = deserialize(valueBytes, 0, valueBytes.length);
            writeToScreen("<span> subscribed key: " + key + " value: " + value + " </span>");
        }
    }
}