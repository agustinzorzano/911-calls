//const elasticsearch = require('elasticsearch');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');

const ELASTIC_SEARCH_URI = 'http://localhost:9200';
const INDEX_NAME = '911-calls';

async function run() {
  const client = new Client({ node: ELASTIC_SEARCH_URI});

  // Drop index if exists
  await client.indices.delete({
    index: INDEX_NAME,
    ignore_unavailable: true
  });

  await client.indices.create({
    index: INDEX_NAME,
    body : {
      "mappings": {
        "properties": {
          "latitude": { "type": "double" },
          "longitude": { "type": "double" },
          "title": { "type": "text", "search_analyzer": "standard" },
          "category": { "type": "keyword" },
          "description": { "type": "text" },
          "zip": { "type": "integer" },
          "timestamp": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" },
          "neighborhood": { "type": "keyword" },
          "address": { "type": "text" },
          "e": { "type": "integer" },
          "location": { "type": "geo_point" },
        }
      }
      // TODO configurer l'index https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html
    }
  });

  let calls = [];

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
      };
      calls.push(call);
      // TODO créer l'objet call à partir de la ligne
    })
    .on('end', async () => {
      const query = createBulkInsertQuery(calls, INDEX_NAME)
      client.bulk(query, (err, resp) => {
        if (err) console.trace(err.message);
        else console.log(`Inserted ${resp.body.items.length} calls`);
        client.close();
      });
      // TODO insérer les données dans ES en utilisant l'API de bulk https://www.elastic.co/guide/en/elasticsearch/reference/7.x/docs-bulk.html
    });
}

function createBulkInsertQuery(calls, index_name) {
  const body = calls.reduce((acc, call) => {
    acc.push({ index: { _index: index_name, _type: '_doc' } })
    acc.push({ ...call })
    return acc
  }, []);

  return { body };
}

run().catch(console.log);


