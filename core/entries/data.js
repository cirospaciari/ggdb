class DataEntry {

    static load(buffer, offset, loadData) {
        offset = offset || 0;
        if(typeof loadData === "undefined")
            loadData = true;

        const size = buffer.readUInt32BE(1 + offset);
        const type = buffer.readUInt8(offset);
        const totalSize = buffer.readUInt32BE(5 + offset);
        return {
            type,
            size,
            totalSize,
            data: loadData ? buffer.toString('utf8', 9 + offset, size + offset + 9 ) : null,
            maxDataSize: totalSize - 9
        }
    }

    static getBuffer(size) {
        let bufferSize = size + 9;
        if (bufferSize < 13) bufferSize = 13; //minimum size
        if(bufferSize < Buffer.poolSize >> 1)
            return Buffer.allocUnsafe(bufferSize).fill(0);
        return Buffer.alloc(bufferSize);
    }
    
    static save(buffer, offset, data, encoding, totalSize) {
        const dataOffset = (9 + offset);
        const size = buffer.write(data, dataOffset, encoding); //data
        buffer.writeUInt32BE(Number(size), 1 + offset); //size
        
        buffer.writeUInt8(1, offset); //data type
        if (size < 4) { //fill minimum size with zeros
            //write zeros
            for (let i = 1; i <= 4 - size; i++){
                buffer.writeInt8(0, dataOffset + size + i);
            }
        }
        if(!totalSize){//if not informed
            totalSize = 9 + (size < 4 ? 4 : size);
        }
        buffer.writeUInt32BE(Number(totalSize), 5 + offset); //total size
        return totalSize;
    }
}

module.exports = DataEntry;