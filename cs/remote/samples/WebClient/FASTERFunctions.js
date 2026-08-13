// JavaScript source code

function writeToScreen(message, className) {
    var output = document.getElementById("output");
    var p = document.createElement("p");
    var span = document.createElement("span");
    if (className) {
        span.className = className;
    }
    // Insert server-supplied data as text, never as HTML, to prevent DOM-based XSS.
    span.textContent = message;
    p.appendChild(span);
    output.insertAdjacentElement("afterbegin", p);
}

class FASTERFunctions extends CallbackFunctionsBase {

    constructor(client) {
        super();
    }

    ReadCompletionCallback(keyBytes, outputBytes, status) {
        if (status == Status.Found) {
            var output = deserialize(outputBytes, 0, outputBytes.length);
            writeToScreen(" value: " + output + " ");
        }
    }

    UpsertCompletionCallback(keyBytes, valueBytes, status) {
        if (status == Status.Found) {
            writeToScreen(" PUT OK ");
        }
    }

    DeleteCompletionCallback(keyBytes, status) { }

    RMWCompletionCallback(keyBytes, outputBytes, status) { }

    SubscribeKVCompletionCallback(keyBytes, outputBytes, status)
    {
        if (status == Status.Found) {
            var key = deserialize(keyBytes, 0, keyBytes.length);
            var output = deserialize(outputBytes, 0, outputBytes.length);
            writeToScreen(" subscribed key: " + key + " value: " + output + " ");
        }
    }

    SubscribeCompletionCallback(keyBytes, valueBytes, status)
    {
        if (status == Status.Found) {
            var key = deserialize(keyBytes, 0, keyBytes.length);
            var value = deserialize(valueBytes, 0, valueBytes.length);
            writeToScreen(" subscribed key: " + key + " value: " + value + " ");
        }
    }
}