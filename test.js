const fs = require("fs");

function getRandomInt(min, max) {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min)) + min; //The maximum is exclusive and the minimum is inclusive
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
        users: new File('users.db')
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
    }
    await db.users.createIndex(100000, "id");
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
  
    console.time("add");
    for (let i = 0; i < 1000; i++) {
        await db.users.add(user); //33~36k p/s
    }
    console.timeEnd("add");
    
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

    //113 p/s (middle of the page - 1k)
    //117k~128k p/s (start of the page - 100k)
    // console.time("Search");
    // for(let i = 0; i < 1000; i++)
    //     await db.users.filter((user)=> user.id === 1, 1);
    // console.timeEnd("Search");
    
    // await db.users.forEach(()=> false);

    // console.time("Index Search");
    // let results;
    //170,940k~217.391k p/s - 100k (1 hop)
    //160.256k p/s - 100k (4 hops)
    //149,031 p/s - 100k (10 hops)
    // for(let i = 0; i < 1; i++)
    //      results = await db.users.filterByIndex({ id: 1 }, null, 1); //51515
    // console.timeEnd("Index Search");
    // console.log(results.length);
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
    console.info("closed");
})();
