
const byteToHex = [];

for (let i = 0; i < 256; ++i) {
    byteToHex.push((i + 0x100).toString(16).substr(1));
}

const helpers = {
    primes: [3, 7, 11, 17, 23, 29, 37, 47, 59, 71, 89, 107, 131, 163, 197, 239, 293, 353, 431, 521, 631, 761, 919,
        1103, 1327, 1597, 1931, 2333, 2801, 3371, 4049, 4861, 5839, 7013, 8419, 10103, 12143, 14591,
        17519, 21023, 25229, 30293, 36353, 43627, 52361, 62851, 75431, 90523, 108631, 130363, 156437,
        187751, 225307, 270371, 324449, 389357, 467237, 560689, 672827, 807403, 968897, 1162687, 1395263,
        1674319, 2009191, 2411033, 2893249, 3471899, 4166287, 4999559, 5999471, 7199369],
    getPrimeCapacity(minimum) {
        let prime = helpers.primes.find(p => p >= minimum);
        if (prime)
            return prime;

        //expand prime if do not have any more primes
        prime = helpers.primes[helpers.primes.length - 1];
        do {
            prime *= 2;
            //if becomes a not safe integer return MAX
            if (!Number.isSafeInteger(prime))
                return Number.MAX_SAFE_INTEGER;
        } while (prime < minimum);

        return prime;
    },
    getBucketIndex(hashcode, bucketSize) {
        return hashcode % bucketSize;
    },
    sortByMostUsedOrLast(a, b) {
        if (Number(a.uses) === Number(b.uses))
            return Number(b.number - a.number);
        return Number(b.uses) - Number(a.uses);
    },

    compare(a, b, sort) {
        if (typeof a === "undefined") {
            if (typeof b === "undefined")
                return 0; //equal
            return -1;
        } else if (typeof b === "undefined") {
            return 1;
        }
        for (let i in sort) {

            if (a[i] === b[i])
                continue;
            if (sort[i] <= -1) {
                return (a[i] > b[i]) ? -1 : 1;
            }
            return (a[i] > b[i]) ? 1 : -1;
        }
        return 0;

    },
    uuidv4() {
        const rnds = require('crypto').randomFillSync(new Uint8Array(16));
        // Per 4.4, set bits for version and `clock_seq_hi_and_reserved`
        rnds[6] = (rnds[6] & 0x0f) | 0x40;
        rnds[8] = (rnds[8] & 0x3f) | 0x80;
        return (
            byteToHex[rnds[0]] +
            byteToHex[rnds[1]] +
            byteToHex[rnds[2]] +
            byteToHex[rnds[3]] +
            '-' +
            byteToHex[rnds[4]] +
            byteToHex[rnds[5]] +
            '-' +
            byteToHex[rnds[6]] +
            byteToHex[rnds[7]] +
            '-' +
            byteToHex[rnds[8]] +
            byteToHex[rnds[9]] +
            '-' +
            byteToHex[rnds[10]] +
            byteToHex[rnds[11]] +
            byteToHex[rnds[12]] +
            byteToHex[rnds[13]] +
            byteToHex[rnds[14]] +
            byteToHex[rnds[15]]
        ).toLowerCase();
    },
    hashCode(value) {

        if (value === null || typeof value === "undefined")
            return 0;
        let hash = 0;
        switch (typeof value) {
            //generate a number hash
            case "bigint":
                value = Number(value);
            case "number":
                hash = value;
                hash |= 0;
                return hash & 0x7FFFFFFF;

            default:
            case "object":
                if (value instanceof Date) {
                    //this is not used anymore because JSON.stringify and JSON.parse    

                    //use getTime do generate hash1
                    // hash = value.getTime();
                    // hash != 0;
                    // return hash & 0x7FFFFFFF;

                    value = value.toISOString(); //index as ISOstring
                } else if (value instanceof RegExp) { //generate string hash
                    value = value.toString();
                } else if (value) { //generate json hash
                    value = JSON.stringify(value);
                } else {
                    //toString
                    value = value + "";
                }
            case "string":
                let i, chr;
                for (i = 0; i < value.length; i++) {
                    chr = value.charCodeAt(i);
                    hash = ((hash << 5) - hash) + chr;
                    hash |= 0; // Convert to 32bit integer
                }
                return hash & 0x7FFFFFFF;
        }
    }
};
module.exports = helpers;