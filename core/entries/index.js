class IndexEntry {

    static load(buffer, offset, start) {
        offset = offset || 0;
        const entry = {
            type: 5,
            start: start,
            property: buffer.toString('utf8', 1 + offset, offset + 255).replace(/\0/g, '').split(';'),
            size: buffer.readUInt32BE(256 + offset)
        };
        return entry;
    }

    static loadExtraEntry(buffer, offset){
        return {
            type: 6,
            hash: buffer.readUInt32BE(offset + 1),
            position: buffer.readUInt32BE(offset + 5),
            page: buffer.readUInt32BE(offset + 9),
            next: buffer.readUInt32BE(offset + 13),
            nextPage: buffer.readUInt32BE(offset + 17)
        }
    }

    static saveExtraEntry(buffer, offset, entry){
        buffer.writeUInt8(6, offset);
        buffer.writeUInt32BE(Number(entry.hash), offset + 1);
        buffer.writeUInt32BE(Number(entry.position), offset + 5);
        buffer.writeUInt32BE(Number(entry.page), offset + 9);
        buffer.writeUInt32BE(Number(entry.next), offset + 13);
        buffer.writeUInt32BE(Number(entry.nextPage), offset + 17);
    }

    static getExtraEntryBuffer() {
        return Buffer.allocUnsafe(IndexEntry.getExtraEntryBufferSize()).fill(0);
    }
    static getBuffer() {
        return Buffer.allocUnsafe(IndexEntry.getBufferSize()).fill(0);
    }
    static getExtraEntryBufferSize() {
        return 21;
    }
    static getBufferSize() {
        return 260;
    }

    static async createBucket(write, index){
        const headerSize = IndexEntry.getBufferSize();
        index.buffer = Buffer.alloc(Number(index.size));//alloc zero fill entries
        await write(index.buffer, 0, Number(index.size), Number(index.start) + headerSize);
        const size = index.size / 20;
        if(!index.entries){
            index.entries = new Array(index.size / 20);
        }
        for(let i = 0; i < size; i++){
            //fill entries
            index.entries[i] = {
                hash: 0,
                position: 0,
                page: 0,
                next: 0,
                nextPage: 0
            }
        }
    }

    static async loadBucket(read, entry) {
        
        if(!entry.buffer){
            const headerSize = IndexEntry.getBufferSize();
            entry.buffer = Buffer.alloc(Number(entry.size));//alloc entries buffer
            await read(entry.buffer, 0, Number(entry.size), Number(entry.start) + headerSize);
        }
        /*
         * 4 bytes hash
         * 8 bytes position  (0 if its a free position)
         * 8 bytes next index in-list position (0 if none)
         */
        if(!entry.entries){
            entry.entries = new Array(entry.size / 20);
        }
         for(let i = 0; i < entry.size; i += 20){ //20 bytes
            entry.entries[i / 20] = {
                hash: entry.buffer.readUInt32BE(i),
                position: entry.buffer.readUInt32BE(i + 4),
                page: entry.buffer.readUInt32BE(i + 8),
                next: entry.buffer.readUInt32BE(i + 12),
                nextPage: entry.buffer.readUInt32BE(i + 16)
            }
        }
    }


    //update one entry (only in memory)
    static updateEntry(index, position, entry){
        index.buffer.writeUInt32BE(Number(entry.hash), (position * 20));
        index.buffer.writeUInt32BE(Number(entry.position), (position * 20) + 4);
        index.buffer.writeUInt32BE(Number(entry.page), (position * 20) + 8);
        index.buffer.writeUInt32BE(Number(entry.next), (position * 20) + 12);
        index.buffer.writeUInt32BE(Number(entry.nextPage), (position * 20) + 16);
    }

    //save all updated entries
    static saveBucket(write, index){
        const headerSize = IndexEntry.getBufferSize();
        return write(index.buffer, 0, Number(index.size), Number(index.start) +  headerSize);
    }

    static save(buffer, offset, start, properties, size) {

        buffer.writeUInt8(5, offset); //type
        let property = properties.join(';');
        property += '\0'.repeat(255 - Buffer.byteLength(property));
        buffer.write(property, 1 + offset, 'utf8');
        buffer.writeUInt32BE(Number(size), 256 + offset);

        return {
            type: 5,
            start: start,
            property: properties,
            size
        };
    }
}

module.exports = IndexEntry;