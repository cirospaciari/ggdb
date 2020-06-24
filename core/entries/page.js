class PageEntry{

    static load(buffer, offset, start){
        offset = offset || 0;
        return {
            type: 4,
            number: buffer.readUInt32BE(1 + offset),
            count: buffer.readUInt32BE(5 + offset),
            start: start,
            end: buffer.readBigUInt64BE(9 + offset),
            next: buffer.readBigUInt64BE(17 + offset),
            uses: buffer.readBigUInt64BE(25 + offset)
            //ignore data because its not important
        } 
    }
    static getBuffer(){
        return Buffer.allocUnsafe(33).fill(0);
    }
    static getBufferSize(){
        return 33;
    }

    static updateCount(write, page){
        const buffer = Buffer.allocUnsafe(4);
        buffer.writeUInt32BE(Number(page.count));
        return write(buffer, 0, 4, Number(page.start) + 5);
    }
    static updateCountAndEnd(write, page){
        const buffer = Buffer.allocUnsafe(12);
        buffer.writeUInt32BE(Number(page.count)); //count
        buffer.writeBigUInt64BE(BigInt(page.end), 4); //end
       return write(buffer, 0, 12, Number(page.start) + 5);
    }
    static updateUses(write, page, buffer, offset){
        buffer = buffer || Buffer.allocUnsafe(8);
        offset = offset || 0;
        buffer.writeBigUInt64BE(BigInt(page.uses), offset);
        return write(buffer, Number(offset), 8, Number(page.start) + 25);
    }
    static save(buffer, offset, number, count, end, next, uses){
        buffer.writeUInt8(4, offset); //type
        buffer.writeUInt32BE(Number(number), 1 + offset); //number
        buffer.writeUInt32BE(Number(count), 5 + offset); //count
        buffer.writeBigUInt64BE(BigInt(end), 9 + offset); //end
        buffer.writeBigUInt64BE(BigInt(next), 17 + offset); //next
        buffer.writeBigUInt64BE(BigInt(uses), 25 + offset); //uses
        return {
            type: 4,
            number,
            count,
            end,
            next,
            uses
        }
    }
}

 module.exports = PageEntry;