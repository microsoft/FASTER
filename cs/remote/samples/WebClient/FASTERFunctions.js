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

    SubscribeKVCompletionCallback(keyBytes, outputBytes, status) { }
}