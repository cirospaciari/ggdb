class SequenceEntry {

    static load(buffer, offset, start) {
        offset = offset || 0;
        return {
            type: 7,
            start: start,
            property: buffer.toString('utf8', 1 + offset, offset + 255).replace(/\0/g, ''),
            startAt: buffer.readBigUInt64BE(256 + offset),
            increment: buffer.readUInt32BE(264 + offset),
            data_type: buffer.readUInt32BE(268 + offset),
            value: buffer.readBigUInt64BE(272 + offset),
            next: buffer.readBigUInt64BE(280 + offset)
        }
    }
    static getBuffer() {
        return Buffer.allocUnsafe(this.getBufferSize()).fill(0);
    }
    static getBufferSize() {
        return 288;
    }

    static increment(write, sequence, buffer, offset) {
        buffer = buffer || Buffer.allocUnsafe(8);
        offset = offset || 0;

        sequence.value = BigInt(sequence.value) + BigInt(sequence.increment);

        buffer.writeBigUInt64BE(BigInt(sequence.value), offset);
        return write(buffer, Number(offset), 8, Number(sequence.start) + 272);
    }

    static save(buffer, offset, sequence) {
        buffer.writeUInt8(7, offset); //type
        let property = sequence.property;

        property += '\0'.repeat(255 - Buffer.byteLength(property));
        buffer.write(property, 1 + offset, 'utf8');
        buffer.writeBigUInt64BE(BigInt(sequence.startAt), 256 + offset);
        buffer.writeUInt32BE(Number(sequence.increment), 264 + offset);
        buffer.writeUInt32BE(Number(sequence.data_type), 268 + offset);
        buffer.writeBigUInt64BE(BigInt(sequence.value), 272 + offset);
        buffer.writeBigUInt64BE(BigInt(sequence.next), 280 + offset);

        return sequence;
    }
}

module.exports = SequenceEntry;