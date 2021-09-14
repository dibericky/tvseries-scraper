'use strict'

require('dotenv').config()

const amqp = require('amqplib')
var axios = require("axios").default;
const { MongoClient } = require('mongodb');
const logger = require('pino')({level: 'debug'})

const TVSERIES_COLLECTION = 'tvseries'

async function connectMongo () {
  logger.info('connecting to MongoDB')
  const client = new MongoClient(process.env.MONGODB_CONN_STRING, { useNewUrlParser: true, useUnifiedTopology: true });
  await client.connect()
  logger.info('connected to MongoDB')
  return client.db()
}

async function getEpisodesByName (name) {
    logger.debug({name}, 'search on IMDb')
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
        logger.error({name}, 'tv serie not found')
        throw new Error('Not Found')
    }
    const {id, numberOfEpisodes, title} = tvSerie
    const matched = id.match(/^\/title\/(?<uniqueId>\w+)\/$/)
    if (!matched) {
        logger.debug({id}, 'Unknown id format from IMDb')
        throw new Error('Unknown id format from IMDb')
    }
    const {uniqueId} = matched.groups
    return {
        serieId: uniqueId,
        numberOfEpisodes,
        title
    }
}

async function saveTvSerie (mongoDb, {serieId, numberOfEpisodes, title}) {
    const date = new Date()
    logger.debug({serieId, numberOfEpisodes, title, date, collection: TVSERIES_COLLECTION}, 'saving of MongoDB')
    const saved = await mongoDb.collection(TVSERIES_COLLECTION).updateOne({
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
    logger.debug({collection: TVSERIES_COLLECTION, saved}, 'saved on MongoDB')
}

async function handleConsume (msg, mongoDb) {
    logger.debug(msg, 'received from RabbitMQ')   
    const tvSerieDetail = await getEpisodesByName(msg.name)
    logger.debug(tvSerieDetail, 'tvseries detail')
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
              handleConsume(JSON.parse(msg.content.toString()), mongoDb)
                .then(() => {
                    logger.info('sending ack')
                    channel.ack(msg)
                })
    }, {
        noAck: false
    })
}

run()