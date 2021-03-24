const {
  MongoClient
} = require('mongodb');
const csv = require('csv-parser');
const fs = require('fs');
const {
  mainModule
} = require('process');

const MONGO_URL = 'mongodb://localhost:27017/';
const DB_NAME = '911-calls';
const COLLECTION_NAME = 'calls';

const insertCalls = async function (db, callback) {
  const collection = db.collection(COLLECTION_NAME);
  await dropCollectionIfExists(db, collection);

  const calls = [];
  fs.createReadStream('../911.csv')
    .pipe(csv())
    .on('data', data => {
      const call = {
        "latitude": data.lat,
        "longitude": data.lng,
        "category": data.title.split(':')[0],
        "description": data.desc,
        "zip": data.zip,
        "title": data.title,
        "timestamp": data.timeStamp,
        "neighborhood": data.twp,
        "address": data.addr,
        "e": data.e,
        "location" : [parseFloat(data.lng), parseFloat(data.lat)]
      }; // TODO créer l'objet call à partir de la ligne

      calls.push(call);
    })
    .on('end', () => {
      collection.insertMany(calls, (err, result) => {
        callback(result)
      });
      // we add the indexes
      collection.createIndex({ title: "text" });
      collection.createIndex( { location : "2dsphere" } );
    });
}

MongoClient.connect(MONGO_URL, {
  useUnifiedTopology: true
}, (err, client) => {
  if (err) {
    console.error(err);
    throw err;
  }
  const db = client.db(DB_NAME);
  insertCalls(db, result => {
    console.log(`${result.insertedCount} calls inserted`);
    client.close();
  });
});

async function dropCollectionIfExists(db, collection) {
  const matchingCollections = await db.listCollections({name: COLLECTION_NAME}).toArray();
  if (matchingCollections.length > 0) {
    await collection.drop();
  }
}

/*
db.calls.createIndex(
    {
      title: "text"
    }
)

db.calls.createIndex( { location : "2dsphere" } )

db.calls.aggregate( [
  {
    $group: {
       _id: "$category",
       count: { $sum: 1 }
    }
  }
] )

*/
