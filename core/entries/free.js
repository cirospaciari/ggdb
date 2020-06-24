class FreeEntry{

    static load(buffer, offset){
        offset = Number(offset || 0);
        const size = buffer.readUInt32BE(1 + offset);
        const totalSize = 13 + size;
        return {
            type: 3,
            size,
            next: buffer.readBigUInt64BE(5 + offset),
            totalSize, 
            maxDataSize: totalSize - 9 
            //ignore data because its not important
        } 
    }
    static zeroFill(buffer, offset, size){
        for(let i = 0;i < size; i++ ){
            buffer.writeUInt8(0, Number(offset) + 13 + i);
        }
    }   

    static getBuffer(){
        return Buffer.allocUnsafe(13).fill(0);
    }
    static getBufferSize(){
        return 13;
    }

    static updateNext(write, position, next){
        const buffer = Buffer.allocUnsafe(8);
        buffer.writeBigUInt64BE(next); //next
        return write(buffer, 0, 8, Number(position) + 5);
    }
    static save(buffer, offset, size, next){
        //transform entry into free entry
        buffer.writeUInt8(Number(3), offset); //type
        buffer.writeUInt32BE(Number(size), 1 + offset); //size
        buffer.writeBigUInt64BE(BigInt(next), 5 + offset); //next
    }
}

 module.exports = FreeEntry;