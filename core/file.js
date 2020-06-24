const CURRENT_VERSION = new Uint8Array([0, 0, 1]);
const HEADER_SIZE = 72;

//https://www.npmjs.com/package/fast-json-stringify
//https://github.com/chrisdickinson/bops

const fs = require('fs');
const util = require('util');
const read = util.promisify(fs.read);
const write = util.promisify(fs.write);
const close = util.promisify(fs.close);
const fstat = util.promisify(fs.fstat);
const open = util.promisify(fs.open);

const { hashCode, getPrimeCapacity, getBucketIndex, sortByMostUsedOrLast, compare, uuidv4 } = require('./helpers');

const DataEntry = require('./entries/data');
const FreeEntry = require('./entries/free');
const PageEntry = require('./entries/page');
const SequenceEntry = require('./entries/sequence');
const IndexEntry = require('./entries/index');
const { Synquer } = require('./synquer');

const Errors = {
    Corrupted: new Error('Corrupted database. Perform an analysis to try fix it.'),
    IncompatibleHeader: new Error('Incompatible database file header.'),
    CharsetTooLong: new Error('Charset name too long for header.'),
    FileIsClosed: new Error(`File is closed`)
}

class File {

    constructor(filename, header) {
        this.filename = filename;
        this.write = this.write.bind(this);
        this.isLoading = true;
        //queue for all writes in file
        this.write_queue = new Synquer();
        //update can have its own queue but if data dont fit its do a insert/delete operation
        this.update_queue = new Synquer();
        //insert and delete shares the same queue
        this.insert_delete_queue = new Synquer();
        //queue for page creation
        this.page_queue = new Synquer();
        this.header = header;
        this.IOMode = "async";;
        this.loaded_pages = {};
        this.loaded_pages_counter = 0;
        this.sequences = [];
        this.indexes = [];
        this.pagesByIndex = {};
    }



    async open() {

        try {
            this.fd = this.IOMode === "async" ? await open(this.filename, 'r+') : fs.openSync(this.filename, "r+");
            await this.loadHeaderBuffer();
            await this.loadHeader();
            await this.loadPagesAndIndexes();
            this.isLoading = false;

        } catch (err) {
            if (err.code !== 'EEXIST' && err.code !== 'ENOENT') {
                throw err;
            }
            this.fd = this.IOMode === "async" ? await open(this.filename, 'w+') : fs.openSync(this.filename, "w+");
            this.header = this.header || {};
            await this.createHeader(this.header.charset, this.header.inMemoryPages, this.header.pageSize);
            this.pages = [];
            this.isLoading = false;

        }
    }

    async loadHeaderBuffer() {
        const stats = await this.fstat({ bigint: true });
        if (stats.size < HEADER_SIZE)
            throw Errors.IncompatibleHeader;

        const buffer = Buffer.alloc(HEADER_SIZE);
        await this.read(buffer, 0, HEADER_SIZE, 0);
        this.header_buffer = buffer;
    }

    loadHeader() {
        const buffer = this.header_buffer;

        const version = [
            buffer.readUInt8(0),
            buffer.readUInt8(1),
            buffer.readUInt8(2)
        ];
        //wrong version
        if (version.some((v, i) => CURRENT_VERSION[i] !== v)) {
            throw new Error(`File db version v${version.join(".")} is not compatible with v${CURRENT_VERSION.join(".")}`)
        }

        this.header = {
            version,
            charset: buffer.toString('utf8', 3, 19).replace(/\0/g, ''),
            nextFreePosition: buffer.readBigUInt64BE(19),
            count: buffer.readBigUInt64BE(27),
            spaceUsed: buffer.readBigUInt64BE(35),
            inMemoryPages: buffer.readUInt8(43),
            pageSize: buffer.readUInt32BE(44),
            nextIndexInfo: buffer.readBigUInt64BE(48),
            nextPageInfo: buffer.readBigUInt64BE(56),
            nextSequenceInfo: buffer.readBigUInt64BE(64),
        }


    }

    createHeader(charset, inMemoryPages, pageSize) {

        this.header = {
            version: CURRENT_VERSION,
            nextFreePosition: 0n, //0 = end of file
            charset: charset || 'utf8',
            count: 0n,
            spaceUsed: 0n,
            inMemoryPages: inMemoryPages || 100,
            pageSize: pageSize || 10000,
            nextIndexInfo: 0n,
            nextPageInfo: 0n,
            nextSequenceInfo: 0n
        }

        this.header.charset = this.header.charset || 'utf8';

        const buffer = Buffer.alloc(HEADER_SIZE);
        //version
        buffer.writeUInt8(this.header.version[0], 0);
        buffer.writeUInt8(this.header.version[1], 1);
        buffer.writeUInt8(this.header.version[2], 2);

        //charset
        const charset_size = buffer.write(this.header.charset, 3, 'utf8') - 3;
        if (charset_size > 16)
            throw Errors.CharsetTooLong;
        //nextFreePosition
        buffer.writeBigUInt64BE(this.header.nextFreePosition, 19);
        //count
        buffer.writeBigUInt64BE(this.header.count, 27);
        //spaceUsed
        buffer.writeBigUInt64BE(this.header.spaceUsed, 35);
        //inMemoryPages
        buffer.writeUInt8(this.header.inMemoryPages, 43);
        //pageSize
        buffer.writeUInt32BE(this.header.pageSize, 44);
        //nextIndexInfo
        buffer.writeBigUInt64BE(this.header.nextIndexInfo, 48);
        //nextPageInfo
        buffer.writeBigUInt64BE(this.header.nextPageInfo, 56);
        //nextSequenceInfo
        buffer.writeBigUInt64BE(this.header.nextSequenceInfo, 64)

        this.header_buffer = buffer;

        return this.write(buffer, 0, buffer.length, 0);
    }

    updateHeaderCounters() {
        const buffer = this.header_buffer;
        //update data in buffer
        buffer.writeBigUInt64BE(this.header.nextFreePosition, 19);
        buffer.writeBigUInt64BE(this.header.count, 27);
        buffer.writeBigUInt64BE(this.header.spaceUsed, 35);
        //save in disk
        return this.write(buffer, 19, 24, 19); //write only nextFreePosition, count, and spaceUsed
    }

    updateHeaderInfo() {
        const buffer = this.header_buffer;
        //update data in buffer
        buffer.writeBigUInt64BE(this.header.nextIndexInfo, 48);
        buffer.writeBigUInt64BE(this.header.nextPageInfo, 56);
        buffer.writeBigUInt64BE(this.header.nextSequenceInfo, 64);
        //save in disk
        return this.write(buffer, 48, 24, 48); //write only index and page info
    }

    fstat(...parameters) {
        if (this.IOMode === "async")
            return fstat(this.fd, ...parameters);
        return fs.fstatSync(this.fd, ...parameters);
    }

    write(...parameters) {
        if (this.IOMode === "async")
            return this.write_queue.execute(() => write(this.fd, ...parameters));
        //use queue because can change async to sync on a opened file so... need to drain the queue first
        return this.write_queue.execute(() => fs.writeSync(this.fd, ...parameters));
    }
    read(...parameters) {
        if (this.IOMode === "async")
            return read(this.fd, ...parameters);
        return fs.readSync(this.fd, ...parameters);
    }

    async waitLoading() {
        return new Promise((resolve) => {
            const check = () => {
                if (this.isLoading) {
                    setImmediate(check);
                } else {
                    resolve();
                }
            }
            setImmediate(check);
        });
    }

    async loadPagesAndIndexes() {

        this.pages = [];
        this.pagesByIndex = {};
        let next = this.header.nextPageInfo;
        while (next != 0n) {
            const buffer = PageEntry.getBuffer();
            await this.read(buffer, 0, buffer.byteLength, Number(next));
            const page = PageEntry.load(buffer, 0, next);
            next = page.next;
            this.pages.push(page);
            this.pagesByIndex[page.number] = page;
        }
        this.lastPage = this.pages[this.pages.length - 1];
        //sort most used
        this.pages.sort(sortByMostUsedOrLast);
        //load in memory most used
        this.loaded_pages = this.loaded_pages || {};
        this.loaded_pages_counter = 0;
        for (let i = 0; i < this.header.inMemoryPages && i < this.pages.length; i++) {
            this.loadPage(this.pages[i]);
        }

        //load sequences
        next = this.header.nextSequenceInfo;
        while (next != 0n) {
            const buffer = SequenceEntry.getBuffer();
            await this.read(buffer, 0, buffer.byteLength, Number(next));
            const sequence = SequenceEntry.load(buffer, 0, next);
            next = sequence.next;
            this.sequences.push(sequence);
        }


        // this.indexes = []; 
        // next = this.header.nextIndexInfo;
        // while (next != 0n) {
        //     const buffer = Buffer.alloc(269);
        //     fs.readSync(this.fd, buffer, 0, 269, next);
        //     const index = IndexEntry.load(buffer, 0, next);
        //     next = index.next;
        //     this.indexes.push(index);
        // }
    }

    async createSequence(property, options) {
        if (this.closed)
            throw Errors.FileIsClosed;
        if (this.isLoading) {
            await this.waitLoading();
        }
        if (this.sequences.some((sequence) => sequence.property == property)) {
            throw new Error(`sequence with the name '${property}' already exists`);
        }
        options = options || { start: 1, increment: 1, type: 'Number' };
        const start = options.start || 1;
        const increment = options.increment || 1;
        let type = 1;
        switch (options.type) {
            case 'guid':
            case 'Guid':
            case 'GUID':
            case 'uuid':
            case 'UUID':
                type = 3;
                break
            case 'Date':
                type = 2;
                break;
            case 'Number':
            default:
                type = 1;
                break;
        }


        return await this.insert_delete_queue.execute(async () => {
            const buffer = SequenceEntry.getBuffer();
            const position = await this.getFreePosition(buffer.byteLength, false, true);

            const sequence = {
                property: property,
                startAt: start,
                value: start - increment,
                increment,
                data_type: type,
                next: BigInt(this.header.nextSequenceInfo)
            };
            SequenceEntry.save(buffer, 0, sequence);

            const promises = [];
            if (position.isEOF) {
                promises.push(this.write(buffer, 0, buffer.byteLength, Number(position.page.end)));
                //expand in memory 
                sequence.start = position.page.end;
                position.page.end = BigInt(position.page.end) + BigInt(buffer.byteLength);
                promises.push(PageEntry.updateCountAndEnd(this.write, position.page));
            } else {
                promises.push(this.write(entryDataBuffer, Number(position.page.offset), entryDataBuffer.byteLength, Number(position.page.absolute_offset)));
                //update nextFreePosition
                if (position.before != 0n) {
                    promises.push(FreeEntry.updateNext(this.write, position.before, position.after));
                }
                sequence.start = position.page.absolute_offset;
            }
            this.sequences.push(sequence);

            //update header
            this.header.spaceUsed += BigInt(buffer.byteLength);
            this.header.nextSequenceInfo = BigInt(sequence.start);
            //write header on disk
            promises.push(this.updateHeaderCounters());
            promises.push(this.updateHeaderInfo());
            await Promise.all(promises);
            return sequence;
        });
    }

    updatePageUsage(page) {
        page.uses = Date.now();
        return PageEntry.updateUses(this.write, page);
    }

    async loadPage(page) {
        this.loaded_pages = this.loaded_pages || {};
        this.loaded_pages_counter = this.loaded_pages_counter || 0;
        let buffer = this.loaded_pages[page.number];
        if (!buffer) {
            buffer = Buffer.alloc(Number(page.end - page.start));
            await this.read(buffer, 0, buffer.byteLength, Number(page.start));
            this.loaded_pages[page.number] = buffer;
            this.loaded_pages_counter++;
        }

        page.buffer = buffer;
        return page;
    }

    async createPage(stats) {
        //no page available create new!
        return await this.page_queue.execute(async () => {
            const promises = [];
            let page_buffer = PageEntry.getBuffer();
            //page number starts in 1
            let page = PageEntry.save(page_buffer, 0, this.pages.length + 1, 0n, BigInt(stats.size) + BigInt(page_buffer.byteLength), 0n, Date.now());
            page.start = stats.size;
            promises[0] = this.write(page_buffer, 0, page_buffer.byteLength, Number(page.start));

            if (this.lastPage) {
                this.lastPage.next = page.start;
                page_buffer = page_buffer.fill(0);
                PageEntry.save(page_buffer, 0, this.lastPage.number, this.lastPage.count, this.lastPage.end, this.lastPage.next, this.lastPage.uses);
                //save new page position
                promises[1] = this.write(page_buffer, 0, page_buffer.byteLength, Number(this.lastPage.start));
            } else {
                //save new page position if its the first
                this.header.nextPageInfo = page.start;
                promises[2] = this.updateHeaderInfo();
            }
            this.lastPage = page;
            this.pagesByIndex = this.pagesByIndex || {};
            this.pagesByIndex[page.number] = page;
            this.pages.push(page);
            //keep order
            this.pages.sort(sortByMostUsedOrLast);


            await Promise.all(promises);
            return page;
        });
    }

    getPositionPage(position, EOF) {
        //if position its end of file returns last page
        if (Number(position) == Number(EOF)) {
            return this.lastPage;
        }
        return this.pages.find(page => position >= page.start && position < page.end);
    }

    async getEndOfFilePosition() {
        let stats = await this.fstat({ bigint: true });
        let before = 0n;
        let after = 0n;
        let position = stats.size;
        let page = this.lastPage; //if its end of file use last page

        if (!page) {
            //if its dont find a page create it!
            page = await this.createPage(stats);
        }
        //update file size
        return { page, offset: position - page.start, absolute_offset: position, before, after, isEOF: true, entry_pre_alloc_size: 0 };
    }

    async getFreePosition(size, itsForDataInserting, itsTotalSize) {
        let stats = await this.fstat({ bigint: true });
        let before = 0n;
        let after = 0n;
        let position = stats.size;
        let page = this.lastPage; //if its end of file use last page
        let entry_pre_alloc_size = 0;
        let nextFree = this.header.nextFreePosition;
        while (nextFree !== 0n) {
            let last = 0n;
            page = this.getPositionPage(nextFree, stats.size);
            if (!page)
                throw Errors.Corrupted;

            let free = null;
            if (this.loaded_pages[page.number]) {
                free = FreeEntry.load(this.loaded_pages[page.number], nextFree - page.start);
            } else {
                const buffer = FreeEntry.getBuffer();
                await this.read(buffer, 0, buffer.byteLength, Number(nextFree));
                free = FreeEntry.load(buffer, 0);
            }
            if ((itsTotalSize ? free.totalSize >= size : free.maxDataSize >= size) && (!itsForDataInserting || page.count < this.header.pageSize)) {
                before = last;
                after = free.next;
                position = nextFree;

                entry_pre_alloc_size = free.totalSize;
                break;
            }
            page = null;
            last = nextFree;
            nextFree = free.next;
        }
        const isEOF = position === stats.size;

        if (!page) {
            //if its dont find a page create it!
            page = await this.createPage(stats);
        }
        //update file size
        return { page, offset: position - page.start, absolute_offset: position, before, after, isEOF, entry_pre_alloc_size };
    }

    async bulkAdd(objArray) {
        if (this.closed)
            throw Errors.FileIsClosed;
        if (this.isLoading) {
            await this.waitLoading();
        }
        const start = Date.now();
        return await this.insert_delete_queue.execute(async () => {

            const end = Date.now();
            return {
                timing: end - start,
            }
        });
    }

    async add(obj) {
        if (this.closed)
            throw Errors.FileIsClosed;
        if (this.isLoading) {
            await this.waitLoading();
        }
        return await this.insert_delete_queue.execute(async () => {
            const promises = [];
            //increment and add sequence info
            this.sequences.forEach((sequence) => {
                switch (sequence.data_type) {
                    case 2: //Date
                        obj[sequence.property] = new Date();
                        break;
                    case 3: //uuid v4
                        obj[sequence.property] = uuidv4();
                        break;
                    default:
                    case 1:
                        //Number
                        promises.push(SequenceEntry.increment(this.write, sequence));
                        obj[sequence.property] = Number(sequence.value);
                        break;
                }
            });
            //serialize data
            let data = JSON.stringify(obj);
            const size = Buffer.byteLength(data);
            let position = await this.getFreePosition(size, true);

            const entryDataBuffer = DataEntry.getBuffer(size);
            DataEntry.save(entryDataBuffer, 0, data, this.header.charset, position.entry_pre_alloc_size);
            let absolute_position = 0;

            if (position.isEOF) {
                //write on disk
                promises.push(this.write(entryDataBuffer, 0, entryDataBuffer.byteLength, Number(position.page.end)));
                absolute_position = Number(position.page.end);
                //expand in memory 
                position.page.end = BigInt(position.page.end) + BigInt(entryDataBuffer.byteLength);
                position.page.count++;
                //write new page count and end on disk
                promises.push(PageEntry.updateCountAndEnd(this.write, position.page));
            } else {
                absolute_position = Number(position.absolute_offset);
                //Update free entry into a data entry
                //write on disk\
                promises.push(this.write(entryDataBuffer, 0, entryDataBuffer.byteLength, Number(position.absolute_offset)));

                //update nextFreePosition
                if (position.before != 0n) {
                    promises.push(FreeEntry.updateNext(this.write, position.before, position.after));
                } else {
                    this.header.nextFreePosition = position.after;
                }
                position.page.count++;
                //write page count on disk
                promises.push(PageEntry.updateCount(this.write, position.page));
                // await PageEntry.updateCount(this.write, position.page);
            }
            promises.push(this.updatePageUsage(position.page));
            //update header
            this.header.count++;
            this.header.spaceUsed += BigInt(size);
            //write header on disk
            promises.push(this.updateHeaderCounters());
            // await this.updateHeaderCounters();
            //wait writes in disk
            await Promise.all(promises);

            //unload page if its loaded
            if (this.loaded_pages[position.page.number]) {
                delete position.page.buffer;
                delete this.loaded_pages[position.page.number];
                this.loaded_pages_counter--;
            }
            //search cache
            position.page.loaded_entries = 0;
            delete position.page.entries;
            delete position.page.position_cache;

            const page = position.page;

            await Promise.all(this.indexes.map(async (index) => {


                return index.queue.execute(async () => {
                    let hash = 0;
                    if (index.property.length === 1) {
                        hash = hashCode(obj[index.property[0]]);
                    } else {
                        for (let i = 0; i < index.property.length; i++) {
                            hash = ((hash << 5) - hash) + hashCode(obj[index.property[i]]);
                            hash |= 0; // Convert to 32bit integer
                        }
                        hash = hash;
                        hash = hash & 0x7FFFFFFF;
                    }
                    const bucket_size = index.entries.length;

                    const bucket_index = getBucketIndex(hash, bucket_size);
                    const bucket_entry = index.entries[bucket_index];
                    const bucket_offset = bucket_index * 20;
                    if (bucket_entry.page === 0) { //its a free entry
                        bucket_entry.page = page.number;
                        bucket_entry.position = Number(absolute_position) - Number(page.start);
                        bucket_entry.hash = hash;
                        //update in buffer
                        IndexEntry.updateEntry(index, bucket_index, bucket_entry);
                        await this.write(index.buffer, bucket_offset, 20, index.start + IndexEntry.getBufferSize() + bucket_offset);
                    } else {
                        //create extra entry with the current info
                        const bucket_entry_buffer = IndexEntry.getExtraEntryBuffer();
                        const bucket_extra_entry = {
                            ...bucket_entry
                        };

                        //do not reutilize free spaces for insert/update performance
                        const position = await this.getEndOfFilePosition();
                        IndexEntry.saveExtraEntry(bucket_entry_buffer, 0, bucket_extra_entry);
                        //unload page if its loaded
                        if (position.page.number !== page.number) {
                            this.cleanPage(position.page, true);
                        }
                        //write on disk
                        await this.write(bucket_entry_buffer, 0, bucket_entry_buffer.byteLength, Number(position.absolute_offset));

                        bucket_entry.hash = hash;
                        bucket_entry.position = Number(absolute_position) - Number(page.start);
                        bucket_entry.page = page.number;
                        bucket_entry.next = Number(position.offset);
                        bucket_entry.nextPage = position.page.number;
                    
                        //expand in memory 
                        position.page.end = BigInt(position.page.end) + BigInt(bucket_entry_buffer.byteLength);

                        //write new page count and end on disk
                        await PageEntry.updateCountAndEnd(this.write, position.page);

                        IndexEntry.updateEntry(index, bucket_index, bucket_entry);
                        await this.write(index.buffer, bucket_offset, 20, index.start + IndexEntry.getBufferSize() + bucket_offset);  
                    }
                });

            }));


            return obj;
        });
    }

    async deleteIndex(...properties) {
        if (this.closed)
            throw Errors.FileIsClosed;

        if (this.isLoading) {
            await this.waitLoading();
        }
        const key = properties.sort().join(';');
        const index = this.indexes.find((index) => index.property.join(';') === key);
        if (!index) {
            throw new Error(`Index for properties '${properties.join(', ')}' dot not exists`);
        }

        //just mark as free but this space will not be reused nor the extra entries
        //a rebase and/or reorder of free space will be implemented in future for otimize the used space
        const buffer = FreeEntry.getBuffer();
        const free_entry_size = index.size + index.getBufferSize() - buffer.byteLength;
        //Free data entry
        FreeEntry.save(buffer, 0, free_entry_size, 0n);
        await this.write(buffer, 0, buffer.byteLength, Number(index.start));
    }

    async growIndex(size, ...properties) {
        await this.deleteIndex(...properties);
        await this.createIndex(size, ...properties);
    }


    async createIndex(size, ...properties) {
        if (this.closed)
            throw Errors.FileIsClosed;

        if (this.isLoading) {
            await this.waitLoading();
        }
        const key = properties.sort().join(';');
        if (this.indexes.some((index) => index.property.join(';') == key)) {
            throw new Error(`Index for properties '${properties.join(', ')}' already exists`);
        }

        size = getPrimeCapacity(Math.max(Number(size) || 100000, Number(this.header.count)));

        let index = null;
        await this.insert_delete_queue.execute(async () => {
            let stats = await this.fstat({ bigint: true });
            const buffer = IndexEntry.getBuffer();
            //if a page exists just update the page end
            index = IndexEntry.save(buffer, 0, Number(stats.size) + 1, properties, size * 20);
            await this.write(buffer, 0, buffer.byteLength, index.start);
            await IndexEntry.createBucket(this.write, index);
            index.queue = new Synquer();//update/insert/delete queue for this index
            this.indexes.push(index);

            //create another page if need
            if (this.lastPage) {
                stats = await this.fstat({ bigint: true });
                await this.createPage(stats);
            }

            await index.queue.execute(async () => {
                //update all indexes!
                if (this.header.count > 0) {
                    const bucket_size = index.entries.length;
                    let cleanBuffer = false;
                    let last_page = null;
                    await this.search(() => true, async (data, count, page, buffer, buffer_offset, entry, absolute_position) => {

                        if (last_page && last_page.number !== page.number) {
                            this.cleanPage(last_page, cleanBuffer);
                        }
                        last_page = page;
                        cleanBuffer = false;


                        let hash = 0;
                        if (index.property.length === 1) {
                            hash = hashCode(data[index.property[0]]);
                        } else {
                            for (let i = 0; i < index.property.length; i++) {
                                hash = ((hash << 5) - hash) + hashCode(data[index.property[i]]);
                                hash |= 0; // Convert to 32bit integer
                            }
                            hash = hash;
                            hash = hash & 0x7FFFFFFF;
                        }

                        const bucket_index = getBucketIndex(hash, bucket_size);
                        const bucket_entry = index.entries[bucket_index];
                        if (bucket_entry.page === 0) { //its a free entry
                            bucket_entry.page = page.number;
                            bucket_entry.position = Number(absolute_position) - Number(page.start);
                            //update in buffer
                            IndexEntry.updateEntry(index, bucket_index, bucket_entry);
                        } else {
                            //create extra entry with the current info
                            const bucket_entry_buffer = IndexEntry.getExtraEntryBuffer();
                            const bucket_extra_entry = {
                                ...bucket_entry
                            };

                            //do not reutilize free spaces for insert/update performance
                            const position = await this.getEndOfFilePosition();
                            IndexEntry.saveExtraEntry(bucket_entry_buffer, 0, bucket_extra_entry);
                            //unload page if its loaded
                            if (position.page.number === page.number) {
                                cleanBuffer = true;
                            } else {
                                this.cleanPage(position.page, cleanBuffer);
                            }
                            //write on disk
                            await this.write(bucket_entry_buffer, 0, bucket_entry_buffer.byteLength, Number(position.absolute_offset));

                            bucket_entry.page = page.number;
                            bucket_entry.position = Number(absolute_position) - Number(page.start);
                            bucket_entry.nextPage = position.page.number;
                            bucket_entry.next = Number(position.offset);
                            
                            //expand in memory 
                            position.page.end = BigInt(position.page.end) + BigInt(bucket_entry_buffer.byteLength);
                            //write new page count and end on disk
                            await PageEntry.updateCountAndEnd(this.write, position.page);

                            IndexEntry.updateEntry(index, bucket_index, bucket_entry);
                        }


                    });
                    if (last_page) {
                        //remove search cache
                        this.cleanPage(last_page, cleanBuffer);
                    }
                    //save indexes!
                    await IndexEntry.saveBucket(this.write, index);
                }
            });


        });


    }

    async searchByIndex(condition, filter, action) {
        if (typeof action !== "function" || typeof filter !== "function")
            throw Error("filter and action need to be a function");
        if (this.closed)
            throw Errors.FileIsClosed;

        if (this.isLoading) {
            await this.waitLoading();
        }
        const properties = Object.keys(condition).sort();
        const key = properties.join(';');
        const index = this.indexes.find((index) => index.property.join(';') === key);
        if (!index) {
            throw new Error(`Index for properties '${properties.join(', ')}' dot not exists`);
        }


        let hash = 0;
        if (index.property.length === 1) {
            hash = hashCode(condition[index.property[0]]);
        } else {
            for (let i = 0; i < index.property.length; i++) {
                hash = ((hash << 5) - hash) + hashCode(condition[index.property[i]]);
                hash |= 0; // Convert to 32bit integer
            }
            hash = hash;
            hash = hash & 0x7FFFFFFF;
        }

        const bucket_size = index.entries.length;
        const bucket_index = getBucketIndex(hash, bucket_size);
        let bucket_entry = index.entries[bucket_index];

        const match = (a, b) => {
            for (let i in a) {
                if (a[i] !== b[i]) {
                    return false;
                }
            }
            return true;
        }

        let count = 0;
        while (true) {
            if (bucket_entry.page !== 0 && bucket_entry.hash === hash) {

                let entry = null;
                
                let page = this.pagesByIndex[bucket_entry.page];
                page.position_cache = page.position_cache || {};
                const absolute_position = Number(bucket_entry.position) + Number(page.start);
                let data = null;
                let valid = false;

                if (this.loaded_pages[page.number]) {
                    const inmemory_entry = page.position_cache[bucket_entry.position];
                    if (inmemory_entry) {
                        entry = bucket_entry.entry;
                        data = { ...inmemory_entry.data };
                        if (match(condition, data)) { //check
                            valid = await filter(data, count);
                        }
                    } else {
                        const type = this.loaded_pages[page.number].readUInt8(bucket_entry.position);
                        //if is data type
                        if (type === 1) {
                            entry = DataEntry.load(this.loaded_pages[page.number], bucket_entry.position);

                            data = JSON.parse(entry.data);
                            page.position_cache[bucket_entry.position] = { position: absolute_position, data: { ...data }, next: absolute_position + entry.totalSize, entry };
                            if (match(condition, data)) { //check
                                valid = await filter(data, count);
                            }
                        }
                    }

                } else {
                    //page its not loaded BUT have some partial position cache
                    page.position_cache = page.position_cache || {};
                    const inmemory_entry = page.position_cache[bucket_entry.position];
                    if (inmemory_entry) {
                        entry = bucket_entry.entry;
                        data = { ...inmemory_entry.data };
                        if (match(condition, data)) { //check
                            valid = await filter(data, count);
                        }
                    } else {
                        let buffer = DataEntry.getBuffer(0);
                        await this.read(buffer, 0, buffer.byteLength, absolute_position);
                        entry = DataEntry.load(buffer, 0, false);
                        if (entry.type === 1) {
                            buffer = DataEntry.getBuffer(entry.size);
                            await this.read(buffer, 0, buffer.byteLength, absolute_position);
                            entry = DataEntry.load(buffer, 0, true);
                            data = JSON.parse(entry.data);
                            //add partial cache
                            page.position_cache[bucket_entry.position] = { position: absolute_position, data: { ...data }, next: absolute_position + entry.totalSize, entry };

                            if (match(condition, data)) { //check
                                valid = await filter(data, count);
                            }
                        }
                    }
                }

                if (valid !== false) {
                    this.updatePageUsage(page);//update page usage
                    count++;
                    const endSearch = await action(typeof valid !== "boolean" ? valid : data, count, page, bucket_entry.position, entry, absolute_position);
                    if (endSearch){
                        break;
                    }
                }

            }

            if (bucket_entry.nextPage === 0) {
                break;
            }

            let page = this.pagesByIndex[bucket_entry.nextPage];
            if (this.loaded_pages[page.number]) {
                bucket_entry = IndexEntry.loadExtraEntry(this.loaded_pages[page.number], bucket_entry.next);
            } else {
                let buffer = IndexEntry.getExtraEntryBuffer();
                const absolute_position = Number(bucket_entry.next) + Number(page.start);
                await this.read(buffer, 0, buffer.byteLength, absolute_position);
                bucket_entry = IndexEntry.loadExtraEntry(buffer, 0);
            }
        }
        //clean loaded pages or load pages to cache if needed
        this.cleanPages();
    }
    async filterByIndex(condition, filter, limit, skip, sort) {
        if (typeof filter !== "function")
            filter = () => true;
        if (sort && typeof sort !== "object") {
            throw Error("sort need to be a plain object");
        }

        limit = limit || Number(this.header.count);
        skip = skip || 0;

        let results = [];
        let count = 0;
        let skipped = 0;


        const addInOrder = (data) => {
            count++;

            for (let i = 0; i < count && i < results.length; i++) {
                if (compare(data, results[i], sort) == -1 || typeof results[i] === "undefined") {
                    let next = results[i];
                    results[i] = data;
                    for (let j = i + 1; j < count && j < results.length; j++) {
                        const temp = results[j];
                        results[j] = next;
                        next = temp;
                    }
                    //inserted at position i
                    return;
                }
            }

        }


        await this.searchByIndex(condition, filter, (data, row_count) => {
            if (sort) {
                addInOrder(data);
            } else {
                //simple limit, skip without sort
                skipped++;
                if (skip < skipped) {
                    results[count] = data;
                    count++;
                }
                //if count !== limit keep going
                return count === limit;
            }
        });

        if (sort) {

            results = results.slice(skip, count < results.length ? count : results.length);
        } else {
            results.length = count;
        }
        return results;
    }

    cleanPage(page, cleanBuffer) {
        //unload page if its loaded
        if (cleanBuffer && this.loaded_pages[page.number]) {
            delete page.buffer;
            delete this.loaded_pages[page.number];
            this.loaded_pages_counter--;
        }
        //search cache
        page.loaded_entries = 0;
        delete page.entries;
        delete page.position_cache;
    }
    async update(obj, filter, limit) {
        if (this.closed)
            throw Errors.FileIsClosed;
        if (this.isLoading) {
            await this.waitLoading();
        }

        let last_page = null;
        let cleanBuffer = false;

        const search_promises = [];
        await this.search(filter, (data, count, page, buffer, buffer_offset, entry, absolute_position) => {

            search_promises.push(this.update_queue.execute(async () => {
                const promises = [];
                let updated_data = JSON.stringify({ ...data, ...obj });
                const size = Buffer.byteLength(updated_data);

                if (last_page && last_page.number !== page.number) {
                    this.cleanPage(last_page, cleanBuffer);
                }
                last_page = page;
                cleanBuffer = false;

                if (size <= entry.maxDataSize) {
                    //data fit just update
                    DataEntry.save(buffer, buffer_offset, updated_data, this.header.charset, entry.totalSize);
                    promises.push(this.write(buffer, buffer_offset, entry.totalSize, Number(absolute_position)));
                    //just sub the diference os sizes
                    this.header.spaceUsed -= (BigInt(entry.size) - BigInt(size));
                    //write header on disk
                    promises.push(this.updateHeaderCounters());
                } else {
                    //data dont fit :c add and delete
                    promises.push(this.insert_delete_queue.execute(async () => {
                        let position = await this.getFreePosition(size, true);

                        const entryDataBuffer = DataEntry.getBuffer(size);
                        DataEntry.save(entryDataBuffer, 0, updated_data, this.header.charset, position.entry_pre_alloc_size);
                        if (position.isEOF) {
                            //write on disk
                            promises.push(this.write(entryDataBuffer, 0, entryDataBuffer.byteLength, Number(position.page.end)));
                            //expand in memory 
                            position.page.end = BigInt(position.page.end) + BigInt(entryDataBuffer.byteLength);
                            //write new page count and end on disk
                            promises.push(PageEntry.updateCountAndEnd(this.write, position.page));
                        } else {
                            //Update free entry into a data entry
                            //write on disk
                            promises.push(this.write(entryDataBuffer, 0, entryDataBuffer.byteLength, Number(position.absolute_offset)));
                            //update nextFreePosition
                            if (position.before != 0n) {
                                promises.push(FreeEntry.updateNext(this.write, position.before, position.after));
                            } else {
                                this.header.nextFreePosition = position.after;
                            }
                            //write page count on disk
                            promises.push(PageEntry.updateCount(this.write, position.page));
                        }
                        promises.push(this.updatePageUsage(position.page));
                        //unload page if its loaded
                        if (position.page.number === page.number) {
                            cleanBuffer = true;
                        } else {
                            this.cleanPage(position.page, cleanBuffer);
                        }

                        const entryHeaderSize = FreeEntry.getBufferSize();

                        const free_entry_size = entry.totalSize - entryHeaderSize;
                        //Free data entry
                        FreeEntry.save(buffer, buffer_offset, free_entry_size, Number(this.header.nextFreePosition));
                        promises.push(this.write(buffer, buffer_offset, entryHeaderSize, Number(absolute_position)));

                        //update free position
                        this.header.nextFreePosition = BigInt(absolute_position);
                        //just add the diference os sizes
                        this.header.spaceUsed += (BigInt(size) - BigInt(entry.size));

                        //write header on disk
                        promises.push(this.updateHeaderCounters());
                    }));
                }
                await Promise.all(promises);
            }));

            if (search_promises.length >= limit) {
                return true;
            }
        });
        //await all
        await Promise.all(search_promises);
        if (last_page) {
            //remove search cache
            this.cleanPage(last_page, cleanBuffer);
        }
        return search_promises.length;
    }


    async delete(filter, limit) {
        if (this.closed)
            throw Errors.FileIsClosed;
        if (this.isLoading) {
            await this.waitLoading();
        }
        let last_page = null;
        let delete_promises = [];
        await this.search(filter, async (data, count, page, buffer, buffer_offset, entry, position) => {
            delete_promises.push(this.insert_delete_queue.execute(async () => {
                const promises = [];
                const entryHeaderSize = FreeEntry.getBufferSize();
                const size = entry.totalSize - entryHeaderSize;

                //Free data entry
                FreeEntry.save(buffer, buffer_offset, size, Number(this.header.nextFreePosition));
                promises[0] = this.write(buffer, Number(buffer_offset), entryHeaderSize, Number(position));
                if (last_page && last_page.number !== page.number) {
                    //remove search cache
                    last_page.loaded_entries = 0;
                    delete last_page.entries;
                    delete last_page.position_cache;
                }
                last_page = page;

                //update page count
                page.count--;
                promises[1] = PageEntry.updateCountAndEnd(this.write, page);

                //update headers
                this.header.nextFreePosition = BigInt(position);
                this.header.count--;
                this.header.spaceUsed -= BigInt(entry.size);
                promises[2] = this.updateHeaderCounters();
                //await for delete write in disk
                await Promise.all(promises);
            }));
            if (delete_promises.length >= limit) {
                return true;
            }
        });

        await Promise.all(delete_promises); //await for completion
        if (last_page) {
            //remove search cache
            last_page.loaded_entries = 0;
            delete last_page.entries;
            delete last_page.position_cache;
        }
        return delete_promises.length;
    }


    async forEach(action) {
        if (typeof action !== "function")
            throw Error("action need to be a function");

        this.search(() => true, async (data, row_count) => {
            return await action(data, row_count);
        });
    }

    async filter(filter, limit, skip, sort) {
        if (typeof filter !== "function")
            filter = () => true;
        if (sort && typeof sort !== "object") {
            throw Error("sort need to be a plain object");
        }

        limit = limit || Number(this.header.count); //default limit to 1000
        skip = skip || 0;

        let results = [];
        let count = 0;
        let skipped = 0;


        const addInOrder = (data) => {
            count++;

            for (let i = 0; i < count && i < results.length; i++) {
                if (compare(data, results[i], sort) == -1 || typeof results[i] === "undefined") {
                    let next = results[i];
                    results[i] = data;
                    for (let j = i + 1; j < count && j < results.length; j++) {
                        const temp = results[j];
                        results[j] = next;
                        next = temp;
                    }
                    //inserted at position i
                    return;
                }
            }

        }


        await this.search(filter, (data, row_count) => {
            if (sort) {
                addInOrder(data);
            } else {
                //simple limit, skip without sort
                skipped++;
                if (skip < skipped) {
                    results[count] = data;
                    count++;
                }
                //if count !== limit keep going
                return count === limit;
            }
        });

        if (sort) {

            results = results.slice(skip, count < results.length ? count : results.length);
        } else {
            results.length = count;
        }
        return results;
    }

    cleanPages() {

        //clean extra loaded pages by usage
        this.pages.sort(sortByMostUsedOrLast).forEach((p, index) => {
            if (index >= this.header.inMemoryPages) {
                //clear buffers
                delete p.buffer;
                p.loaded_entries = 0;
                delete p.entries;
                delete p.position_cache;
                delete this.loaded_pages[p.number];
                this.loaded_pages_counter = this.header.inMemoryPages;
            } else if (!p.buffer) { //page its not loaded, but its most used!
                this.loadPage(p);//so load the page!
            }

        });
    }

    async search(filter, action) {
        if (typeof action !== "function" || typeof filter !== "function")
            throw Error("filter and action need to be a function");
        if (this.closed)
            throw Errors.FileIsClosed;

        if (this.isLoading) {
            await this.waitLoading();
        }

        let count = 0;
        let endSearch = false;

        //optimize search by most used pages or most recent
        this.pages.sort(sortByMostUsedOrLast);//sort only when queue its empty

        //clone array
        const pages = this.pages.slice(0);
        //TODO: usar add-on c++ para processamento dos buffers com multi-threading
        //https://community.risingstack.com/using-buffers-node-js-c-plus-plus/

        for (let i = 0; i < pages.length; i++) {
            let page = pages[i];
            let position = Number(page.start);

            page = await this.loadPage(page);
            let lastCount = count;
            if (page.count === 0)
                continue;

            let dataEntryCount = 0;
            page.entries = page.entries || [];
            page.position_cache = page.position_cache || {};

            page.loaded_entries = page.loaded_entries || 0;
            if (page.loaded_entries > 0) {
                let buffer_offset = position - Number(page.start);
                let absolute_position = position;
                for (let j = 0; j < page.loaded_entries; j++) {
                    let data = page.entries[j];
                    position = data.next;
                    const entry = data.entry;

                    absolute_position = data.position;
                    buffer_offset = data.position - Number(page.start);
                    data = { ...data.data };
                    const valid = await filter(data, count);
                    if (valid !== false) {
                        count++;
                        endSearch = await action(typeof valid !== "boolean" ? valid : data, count, page, buffer_offset, entry, absolute_position);
                        if (endSearch)
                            break;

                    }
                }
                if (lastCount !== count) { //if find something increase usage
                    this.updatePageUsage(page);
                }
                if (page.loaded_entries === page.count) {
                    continue;
                }
            }

            dataEntryCount = page.loaded_entries;
            // this is needed to clean pages in async without lose pages info and buffer in use
            const buffer = page.buffer;
            while (position <= page.end && !endSearch) {

                let buffer_offset = position - Number(page.start);
                if (buffer.byteLength <= buffer_offset)
                    break;
                let absolute_position = position;
                let type = buffer.readUInt8(buffer_offset);
                let data = null;
                let entry = null;
                switch (type) {
                    //DataEntry
                    case 1:
                        data = page.entries[dataEntryCount];
                        if (data) {
                            data = { ...data.data };
                            position = data.next;
                        } else {
                            data = page.position_cache[buffer_offset];
                            page.entries[dataEntryCount] = data;
                            if (data) {
                                data = { ...data.data };
                                position = data.next;
                            } else {
                                entry = DataEntry.load(buffer, buffer_offset);
                                data = JSON.parse(entry.data);
                                position += entry.totalSize;
                                const inmemory_entry = { position: absolute_position, data, next: position, entry };
                                page.entries[dataEntryCount] = inmemory_entry;
                                page.position_cache[buffer_offset] = inmemory_entry;
                                delete inmemory_entry.entry.data;
                                page.loaded_entries++;
                            }
                        }

                        dataEntryCount++;
                        break;
                    //FreeEntry
                    case 3: //ignore
                        position += FreeEntry.load(buffer, buffer_offset).totalSize;
                        continue;
                    //PageEntry
                    case 4: //ignore
                        position += PageEntry.getBufferSize();
                        continue;

                    // case 5: //index entry
                    //     position += IndexEntry.getBufferSize();
                    //     continue;
                    case 6: //index extra entry
                        position += IndexEntry.getExtraEntryBufferSize();
                        continue;
                    case 7: //ignore
                        position += SequenceEntry.getBufferSize();
                        continue;
                }

                const valid = await filter(data, count);
                if (valid !== false) {
                    count++;
                    endSearch = await action(typeof valid !== "boolean" ? valid : data, count, page, buffer, buffer_offset, entry, absolute_position);
                }
                if (dataEntryCount === page.count) {
                    break;
                }
            }

            if (lastCount !== count) { //if find something increase usage
                this.updatePageUsage(page);
            }
            this.cleanPages();
        }
    }



    async close() {
        //mark as closed and stop all operations
        this.closed = true;

        //wait queue to close
        await Promise.all([
            this.update_queue.wait(),
            this.insert_delete_queue.wait(),
            this.page_queue.wait(),
            this.write_queue.wait()
        ]);
        //dispose
        this.pages.forEach((p) => {
            delete p.buffer;
            p.loaded_entries = 0;
            delete p.entries;
            delete p.position_cache;
        });
        this.pages = [];
        //free loaded pages
        for (let i in this.loaded_pages) {
            delete this.loaded_pages[i];
        }
        if (this.IOMode === "async") {
            //finnaly closes the file
            return await close(this.fd);
        }
        return fs.closeSync(this.fd);
    }
}

module.exports = File;