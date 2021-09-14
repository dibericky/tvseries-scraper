'use strict'

require('dotenv').config()

const amqp = require('amqplib')
var axios = require("axios").default;
const { MongoClient } = require('mongodb');

const TVSERIES_COLLECTION = 'tvseries'

async function connectMongo () {
  const client = new MongoClient(process.env.MONGODB_CONN_STRING, { useNewUrlParser: true, useUnifiedTopology: true });
  await client.connect()
  console.log('CONNECT to mongo')
  return client.db()
}

async function getEpisodesByName (name) {
    const {data} = await axios.request({
        method: 'GET',
        url: 'https://imdb8.p.rapidapi.com/title/find',
        params: {q: name},
        headers: {
        'x-rapidapi-host': 'imdb8.p.rapidapi.com',
        'x-rapidapi-key': process.env.IMDB8_API_KEY
        }
    })
    const tvSerie = data.results[0]
    if (!tvSerie) {
        throw new Error('Not Found')
    }
    const {id, numberOfEpisodes, title} = tvSerie
    const {uniqueId} = id.match(/^\/title\/(?<uniqueId>\w+)\/$/).groups
    return {
        serieId: uniqueId,
        numberOfEpisodes,
        title
    }
}

async function saveTvSerie (mongoDb, {serieId, numberOfEpisodes, title}) {
    const date = new Date()
    await mongoDb.collection(TVSERIES_COLLECTION).updateOne({
        serieId
      }, {
        $set: {
            serieId,
            numberOfEpisodes,
            title,
            updatedAt: date
        },
        $setOnInsert: {
          createdAt: date
        }
      }, {
        upsert: true
      })
}

async function handleConsume (msg, mongoDb) {
    console.log("[x] Received %s", msg)   
    const tvSerieDetail = await getEpisodesByName(msg.name)
    console.log('tvSerieDetail ', tvSerieDetail)
    await saveTvSerie(mongoDb, tvSerieDetail)
}

async function initializeCollection (mongoDb) {
    const createIndex = async () => {
        console.log('Creating index')
        await mongoDb.collection(TVSERIES_COLLECTION).createIndex({
            "serieId": 1
        },
        {
            unique: true
        })
    }
    let indexes = []
    try {
        indexes = await mongoDb.collection(TVSERIES_COLLECTION).indexes()
    } catch (err) {
        console.log('Collection %s does not exist, it will be created', TVSERIES_COLLECTION)
        await createIndex()
        return
    }
    if (!indexes.find(index => index.key.serieId)) {
        await createIndex()
    }
}

async function run(){
    const mongoDb = await connectMongo()
    await initializeCollection(mongoDb)

    const connection = await amqp.connect(process.env.RABBITMQ_CONN_STRING)
    const channel = await connection.createChannel()
    var queue = 'popcorn-planner.tvserie-retrieve';
      
    channel.assertQueue(queue, {
      durable: true
    });
    channel.prefetch(1)
      
    console.log("[*] Waiting for messages in %s. To exit press CTRL+X", queue)
    channel.consume(queue, (msg) => {
              console.log('GET MSG')
              console.clear()
              handleConsume(JSON.parse(msg.content.toString()), mongoDb)
                .then(() => {
                    channel.ack(msg)
                })
    }, {
        noAck: false
    })
}

run()