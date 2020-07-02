const fs = require("fs");

function getRandomInt(min, max) {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min)) + min; //The maximum is exclusive and the minimum is inclusive
}
const snapmem_tags = {};
function snapmem(tag){
    const used = process.memoryUsage().heapUsed / 1024 / 1024;
    snapmem_tags[tag] = used;
}

function snapmemEnd(tag){
    const start = snapmem_tags[tag] || 0;
    const used = process.memoryUsage().heapUsed / 1024 / 1024;

    console.log(`${tag} mem usage: ${Math.round((used-start) * 100) / 100} MB`);
    delete snapmem_tags[tag];
}

function currentMem(){
    const used = process.memoryUsage().heapUsed / 1024 / 1024;
    console.log(`Current mem usage: ${Math.round((used) * 100) / 100} MB`);
}
/*

const context = { globalVar: 1 }; ///we can pass callbacks
vm.createContext(context); (one context per connection or user)

vm.runInContext('globalVar *= 2;', context);
//timeout :D
try {
    return vm.runInNewContext(`while (true) 1`, {}, {timeout: 3})
} catch(err) {
    // err could be a syntax error, timeout, etc
    console.error(err)

    return null
}
//more secure https://github.com/patriksimek/vm2
//some tcp/ip communication :D
//https://gist.github.com/sid24rane/2b10b8f4b2f814bd0851d861d3515a10
//byte code para eletron e node:
//https://hackernoon.com/how-to-compile-node-js-code-using-bytenode-11dcba856fa9
//https://www.npmjs.com/package/bytenode
//https://github.com/OsamaAbbas/bytenode
*/
(async function main() {
    const File = require('./core/file');

    //create db
    // Buffer.poolSize = 256 * 1024;//256KB

    const db = {
        // users: new File('./users.db', {
        //     inMemoryPages: 10,
        //     pageSize: 10000
        // }),
        users: new File('/media/cirospaciari/_dde_data/users.db', {
            inMemoryPages: 10,
            pageSize: 10000
        })
        // users: new File('/media/cirospaciari/CCMSJR/users.db')
    }
    //sync have more performance but block event loop operations, great for single file dump or a database process only service
    db.users.IOMode = "sync";

    //await open all
    await Promise.all(Object.values(db).map((file) => file.open()));
    console.info("opened");
    
    //create if not exists
    if(!db.users.sequences.some((sequence)=> sequence.property === 'id')){
        await db.users.createSequence("id", { start: 1, increment: 1 }); //use sequencial number
        console.info("sequence created");
        await db.users.createIndex(100000, "id");
        console.info("index created");   
    }


    // await db.users.createSequence("created_at", { type: 'Date' }); //use current date
    // await db.users.createSequence("uuid", { type: 'UUID' }); //Generated unique identifier (UUID or Guid)
    // await db.users.createIndex(100000, "uuid");

    const user = {
        id: 0,
        name: 'ciro',
        surname: 'spaciari',
        address: {
            number: 67
        }
    }

    // const WordGenerator = require("@ciro.spaciari/word.generator");
    // const lot_of_users = Array.from(new Array(10), ()=> { return { ...user, password: WordGenerator.generate(2, 3).words.join('@') }});


    // const util = require('util');
    // const { scrypt, randomBytes } = require("crypto");
    // const scryptAsync = util.promisify(scrypt);
    // lot_of_users[0].password = "123456";

    // console.time("scrypt");
    // await Promise.all(lot_of_users.map(async (user)=> {
    //     user.salt = randomBytes(16).toString("ascii"); //keep 16 bytes long
    //     user.password = (await scryptAsync(user.password, user.salt, 64)).toString("ascii"); //keep 64 bytes long
    // }));

    // const result = (await scryptAsync("123456", lot_of_users[0].salt, 64)).toString("ascii") === lot_of_users[0].password;
    // console.log(lot_of_users[0].password, lot_of_users[0].salt, result);
    // console.timeEnd("scrypt");
  
    snapmem("add");
    console.time("add");
    for (let i = 0; i < 1000000; i++) {
        // if(i % 10000 === 0)
        //     user.id++;
        await db.users.add(user); //33~36k p/s
    }
    console.timeEnd("add");
    snapmemEnd("add");
    // await db.users.update({ surname: 'spaciariiiiiiiiii' }, (user)=> user.id === 3, 1);
    // await db.users.update({ surname: 'spaciariiiiiiiiii2222222222' }, (user)=> user.id === 3, 1);
    // await db.users.delete((user)=> user.id === 2, 1);
    // await db.users.add(user); 
    // await db.users.add(user); //deu merda

    // await db.users.forEach((user)=> {
    //    // console.log(user);
    //     if(user === null){
    //         console.log("deu merda")
    //         return true;
    //     }
    //     // if(user.id > 10)
    //         // return true; //break
    // });

    let results;

    // gc();

    // //113 p/s (middle of the page - 1k)
    // //117k~128k p/s (start of the page - 100k)
    snapmem("search");
    console.time("search");
    for(let i = 0; i < 1; i++)
        results = await db.users.filter((user)=> user.id === 52345);
    console.timeEnd("search");
    console.log(results.length);
    snapmemEnd("search");

    // // gc();

    snapmem("search2");
    console.time("search2");
    for(let i = 0; i < 1; i++)
        results = await db.users.filter((user)=> user.id === 52345);
    console.timeEnd("search2");
    console.log(results.length);
    snapmemEnd("search2");


    //170,940k~217.391k p/s - 100k (1 hop)
    //160.256k p/s - 100k (4 hops)
    //149,031 p/s - 100k (10 hops)
    snapmem("Index Search");
    console.time("Index Search");
    for(let i = 0; i < 100000; i++)
         results = await db.users.filterByIndex({ id: 2 }); //51515
    console.timeEnd("Index Search");
    console.log(results.length);
    snapmemEnd("Index Search");
    

    snapmem("Index Search2");
    console.time("Index Search2");
    for(let i = 0; i < 100000; i++)
         results = await db.users.filterByIndex({ id: 2 }); //51515
    console.timeEnd("Index Search2");
    console.log(results.length);
    snapmemEnd("Index Search2");

    
    // await db.users.forEach(()=> false);
    currentMem();
    gc();
    setTimeout(()=> currentMem(), 2000);
    
    // console.time("search");
    // let results = await db.users.filter((user) => true, 1, 0, { id: -1 });
    // console.log(results);
    // console.timeEnd("search");

    // console.time("search");
    // results = await db.users.filter((user) => true, 1, 0, { id: -1 });
    // console.log(results);
    // console.timeEnd("search");

    //await close all
    await Promise.all(Object.values(db).map((file) => file.close()));

    // Object.values(db).forEach((file) => {
    //     fs.unlinkSync(file.filename);
    // });


    console.info("closed");
})();
