[![npm package](https://nodei.co/npm/ggdb.png?downloads=true&downloadRank=true?maxAge=30)](https://nodei.co/npm/jscomet/)

[![NPM version](https://img.shields.io/npm/v/ggdb.svg)](https://img.shields.io/npm/v/ggdb.svg) [![NPM License](https://img.shields.io/npm/l/ggdb.svg)](https://img.shields.io/npm/l/ggdb.svg) [![Downloads](https://img.shields.io/npm/dt/ggdb.svg?maxAge=43200)](https://img.shields.io/npm/dt/ggdb.svg?maxAge=60) [![ISSUES](https://img.shields.io/github/issues/cirospaciari/ggdb.svg?maxAge=60)](https://img.shields.io/github/issues/cirospaciari/ggdb.svg?maxAge=60)

Support me for future versions:

[![BMC](https://cdn.buymeacoffee.com/buttons/default-orange.png)](https://www.buymeacoffee.com/i2yBGw7)

[![PAGSEGURO](https://stc.pagseguro.uol.com.br/public/img/botoes/doacoes/209x48-doar-assina.gif)](https://pag.ae/7VxyJphKt)


Simple but powerful JSON database with index and sequences capabilities, recommended for up to 1 million of registers per file/collection.


    Warning: do not support multiple threads/processes or multiple access to same file, open the file and keep it open, when idle close the file
        
        
How install:

npm install ggdb

# Basics

```javascript
const { File } = require('ggdb');
const db = {
    user: new File('user.db'),
    address: new File('address.db')
}
 //sync have more performance but block event loop operations, great for single file dump or a database process only service
db.users.IOMode = "async"; //async or sync options

//await open all
await Promise.all(Object.values(db).map((file) => file.open()));

//you can check with .sequences or .indexes if some sequence/index exists
if(!db.users.sequences.some((sequence)=> sequence.property === 'id')){
    await db.users.createSequence("id", { start: 1, increment: 1 }); //use sequencial number
    await db.users.createIndex(100000, "id"); //create index with 100k bucket size
    await db.users.createSequence("created_at", { type: 'Date' }); //use current date ( Date.now() )
    // await db.users.createSequence("uuid", { type: 'UUID' }); //Generated unique identifier (UUID or Guid)
}

const user = {
    id: 0, //will be ignored because a sequences will override these value
    name: 'Ciro',
    surname: 'Spaciari'
}
//insert user in users.db file
user = await db.users.add(user);
//insert address and pass user.id
await db.address.add({
    address: 'Av. Whatever',
    number: 1234,
    state: 'SP',
    city: 'São Paulo',
    country: 'BR'
    user_id: user.id
});


//update by index
await db.address.updateByIndex({ id: address.id }, { number: 1164 });  //key, updated data, filter (optional), limit (optional), skip (optional), sort (optional) 

//update using table scan
await db.address.update((address)=> address.id > 10, { number: 1164 }); //key, updated data, filter (optional), limit (optional), skip (optional), sort (optional) 

//update using index + table scan
await db.address.updateByIndex({ user_id: user.id }, { number: 1164 }, (address)=> address.id > 10); //key, updated data, filter (optional), limit (optional), skip (optional), sort (optional) 

//filter using index
const addresses = await db.address.filterByIndex({ user_id: user.id }, (address)=> address.country === 'BR', 10, 0, { created_at: -1 }) //key, filter (optional), limit (optional), skip (optional), sort (optional) 

await db.users.filter((user)=> user.name === 'Ciro', 1); // filter, limit (optional), skip (optional), sort (optional) 

//deleteByIndex
await db.address.deleteByIndex({ id: addresses[i].id }); //key, filter (optional), limit (optional), skip (optional), sort (optional) 

//delete
await db.address.deleteByIndex({ id: addresses[i].id }); //filter, limit (optional), skip (optional), sort (optional) 
await db.users.delete((user)=> user.surname === 'Spaciari'); //filter, limit (optional), skip (optional), sort (optional) 

//also available:
//db.users.count (same parameters as filter , returns number)
//db.users.countByIndex  (same parameters as filterByIndex, returns number)
//db.users.exists (same parameters as filter, returns boolean)
//db.users.existsByIndex (same parameters as filterByIndex, returns boolean)

//await close all
await Promise.all(Object.values(db).map((file) => file.close()));
```