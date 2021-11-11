class ClientNetworkSession {
    constructor(session, address, port) {

        this.address = address;
        this.port = port;
        this.clientSession = session;
        var wsUri = "ws://" + this.address + ":" + this.port;

        this.websocket = new WebSocket(wsUri);
        var self = this;

        this.websocket.binaryType = 'arraybuffer';

        this.websocket.onopen = function (e) {
            writeToScreen("CONNECTED");
        };

        this.websocket.onclose = function (e) {
            alert("Disconnected");
        };

        this.websocket.onmessage = function (e) {
            const view = new DataView(e.data);
            self.clientSession.ProcessReplies(view, e.data);
        };

        this.websocket.onerror = function (e) {
            writeToScreen("<span class=error>ERROR:</span> " + e.data);
        };
    }
}