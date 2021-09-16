'use strict'

require('dotenv').config()

const amqp = require('amqplib')
var axios = require("axios").default;
const { MongoClient } = require('mongodb');
const pinoLogger = require('pino')({level: 'debug'})

const TVSERIES_COLLECTION = 'tvseries'

async function connectMongo (logger, connectionString) {
  logger.info('connecting to MongoDB')
  const client = new MongoClient(connectionString, { useNewUrlParser: true, useUnifiedTopology: true });
  await client.connect()
  logger.info('connected to MongoDB')
  return client
}

async function getEpisodesByName (logger, name, apiKey) {
    logger.debug({name}, 'search on IMDb')
    const {data} = await axios.request({
        method: 'GET',
        url: 'https://imdb8.p.rapidapi.com/title/find',
        params: {q: name},
        headers: {
        'x-rapidapi-host': 'imdb8.p.rapidapi.com',
        'x-rapidapi-key': apiKey
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

async function saveTvSerie (logger, mongoDb, {serieId, numberOfEpisodes, title}) {
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

async function handleConsume (logger, msg, mongoDb, apiKey) {
    logger.debug({msg}, 'received from RabbitMQ')   
    const tvSerieDetail = await getEpisodesByName(logger, msg.name, apiKey)
    logger.debug({tvSerieDetail}, 'tvseries detail')
    await saveTvSerie(logger, mongoDb, tvSerieDetail)
}

async function initializeCollection (logger, mongoDb) {
    const createIndex = async () => {
        logger.info('Creating index')
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
        logger.info('Collection %s does not exist, it will be created', TVSERIES_COLLECTION)
        await createIndex()
        return
    }
    if (!indexes.find(index => index.key.serieId)) {
        await createIndex()
    }
}

async function run(logger, {RABBITMQ_CONN_STRING, MONGODB_CONN_STRING, IMDB8_API_KEY}){
    const mongoDbClient = await connectMongo(logger, MONGODB_CONN_STRING)
    const mongoDb = mongoDbClient.db()
    await initializeCollection(logger, mongoDb)

    const connection = await amqp.connect(RABBITMQ_CONN_STRING)
    const channel = await connection.createChannel()
    var queue = 'popcorn-planner.tvserie-retrieve';
      
    channel.assertQueue(queue, {
      durable: true
    });
    channel.prefetch(1)
      
    logger.debug({queue}, "waiting for messages in queue")
    channel.consume(queue, (msg) => {
              logger.debug({msg}, 'received message')
              handleConsume(logger, JSON.parse(msg.content.toString()), mongoDb, IMDB8_API_KEY)
                .then(() => {
                    logger.info('Sending ack')
                    channel.ack(msg)
                })
    }, {
        noAck: false
    })

    return async () => {
        await mongoDbClient.close()
        await connection.close()
    }
}

module.exports = run

if (require.main === module) {
    run(pinoLogger, process.env)
}
