'use strict'

require('dotenv').config()

const amqp = require('amqplib')
var axios = require("axios").default;
const { MongoClient } = require('mongodb');
const knex = require('knex')
const {omit} = require('ramda')
const pinoLogger = require('pino')({level: 'debug'})

const TVSERIES_COLLECTION = 'tvseries'
const EPISODES_TABLE = 'episodes'

const QUEUE_RETRIEVE = 'popcorn-planner.tvserie-retrieve'
const QUEUE_SAVED = 'popcorn-planner.tvserie-saved'

async function connectMongo (logger, connectionString) {
  logger.info('connecting to MongoDB')
  const client = new MongoClient(connectionString, { useNewUrlParser: true, useUnifiedTopology: true });
  await client.connect()
  logger.info('connected to MongoDB')
  return client
}

async function connectPostgres (logger, connectionString) {
    logger.info('connecting to postgres')
    const client = knex({
        client: 'pg',
        connection: connectionString
      });
    logger.info('connected to postgres')
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

    const {data: seasons} = await axios.request({
        method: 'GET',
        url: 'https://imdb8.p.rapidapi.com/title/get-seasons',
        params: {tconst: uniqueId},
        headers: {
        'x-rapidapi-host': 'imdb8.p.rapidapi.com',
        'x-rapidapi-key': apiKey
        }
    })
    const allEpisodes = seasons.reduce((acc, season) => {
        return acc.concat(season.episodes)
    }, [])
    return {
        serieId: uniqueId,
        allEpisodes: allEpisodes,
        title
    }
}

async function saveTvSerie (logger, mongoDb, postgresClient, {serieId, allEpisodes, title}) {
    const date = new Date()
    logger.debug({serieId, numberOfEpisodes: allEpisodes.length, title, date, collection: TVSERIES_COLLECTION}, 'saving of MongoDB')
    const saved = await mongoDb.collection(TVSERIES_COLLECTION).updateOne({
        serieId
      }, {
        $set: {
            serieId,
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
    await postgresClient(EPISODES_TABLE).insert(
        allEpisodes.map(episode => ({serieId: serieId, season: episode.season, episode: episode.episode}))
    )
}

async function handleConsume (logger, msg, mongoDb, apiKey, postgresClient) {
    logger.debug({msg}, 'received from RabbitMQ')   
    const tvSerieDetail = await getEpisodesByName(logger, msg.name, apiKey)
    logger.debug({tvSerieDetail: omit(['allEpisodes'], tvSerieDetail)}, 'tvseries detail')
    await saveTvSerie(logger, mongoDb, postgresClient, tvSerieDetail)
    return tvSerieDetail
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

async function initializeTables(logger, client) {
    logger.debug('initializing postgres table')
    const hasTable = await client.schema.hasTable(EPISODES_TABLE)
    if (!hasTable) {
        logger.debug('creating episodes table')
        await client.schema.createTable(EPISODES_TABLE, (table) => {
            table.increments('id')
            table.string('serieId')
            table.integer('season')
            table.integer('episode')
            table.unique(['serieId', 'season', 'episode'])
        })
    }
}

async function run(logger, {RABBITMQ_CONN_STRING, MONGODB_CONN_STRING, IMDB8_API_KEY, POSTGRES_CONN_STRING}){
    const mongoDbClient = await connectMongo(logger, MONGODB_CONN_STRING)
    const mongoDb = mongoDbClient.db()
    await initializeCollection(logger, mongoDb)

    const postgresClient = await connectPostgres(logger, POSTGRES_CONN_STRING)
    await initializeTables(logger, postgresClient)

    const connection = await amqp.connect(RABBITMQ_CONN_STRING)
    const channel = await connection.createChannel()
      
    channel.assertQueue(QUEUE_RETRIEVE, {
      durable: true
    });
    channel.prefetch(1)
      
    logger.debug({queue: QUEUE_RETRIEVE}, "waiting for messages in queue")
    channel.consume(QUEUE_RETRIEVE, (msg) => {
              logger.debug({msg}, 'received message')
              handleConsume(logger, JSON.parse(msg.content.toString()), mongoDb, IMDB8_API_KEY, postgresClient)
                .then(({title}) => {
                    const savedMsg = JSON.stringify({title})

                    logger.debug({queue: QUEUE_SAVED, title}, 'sending message that title has been saved')

                    channel.assertQueue(QUEUE_SAVED, {
                        durable: true
                    })
                    channel.sendToQueue(QUEUE_SAVED, Buffer.from(savedMsg), {
                        persistent: true
                    })

                    logger.info('Sending ack')
                    channel.ack(msg)
                })
    }, {
        noAck: false
    })

    return async () => {
        await mongoDbClient.close()
        await connection.close()
        await postgresClient.destroy()
    }
}

module.exports = run

if (require.main === module) {
    run(pinoLogger, process.env)
}
