class IntSerializer {
    serialize(byteArr, arrIdx, val) {
        var index = arrIdx;
        const byteView = new Uint8Array(byteArr);
        for (index = arrIdx; index < arrIdx + 4; index++) {
            var byte = val & 0xff;
            byteView.set([byte], index);
            val = (val - byte) / 256;
        }

        return index;
    }

    deserialize(byteArr, arrIdx) {
        var index = arrIdx;
        const byteView = new Uint8Array(byteArr);
        var val = 0;
        var tmpVal = 0;
        for (index = arrIdx; index < arrIdx + 4; index++) {
            var byte = byteView[index];
            tmpVal = byte * (256 ** (index - arrIdx));
            val = val + tmpVal;
        }

        return val;
    }
}

class ParameterSerializer {

    constructor() {
        this.intSerializer = new IntSerializer();
    }

    WriteOpSeqNum(byteArr, arrIdx, opSeqNum) {
        return this.intSerializer.serialize(byteArr, arrIdx, opSeqNum);
    }

    WriteKVI(byteArr, arrIdx, objBytes, lenObj) {
        const byteView = new Uint8Array(byteArr);
        arrIdx = this.intSerializer.serialize(byteArr, arrIdx, lenObj);

        for (var index = 0; index < lenObj; index++) {
            byteView[arrIdx + index] = objBytes[index];
        }

        var endIdx = arrIdx + lenObj;
        return endIdx;
    }

    ReadOutput(byteArr, arrIdx) {
        var lenOutput = this.intSerializer.deserialize(byteArr, arrIdx);
        const byteView = new Uint8Array(byteArr);

        arrIdx += 4;
        var output = [];

        for (var index = 0; index < lenOutput; index++) {
            output[index] = byteView[arrIdx + index];
        }

        return output;
    }
}
