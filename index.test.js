'use strict'

const tap = require('tap')
const {MongoClient} = require('mongodb')
const amqp = require('amqplib')
const logger = require('pino')({level: 'silent'})
const nock = require('nock')
const {omit} = require('ramda')
const sinon = require('sinon')
const knex = require('knex')

const mockAPIFind = require('./find-title.example.json')
const mockAPIEpisodes = require('./get-episodes.example.json')

const main = require('./index')

const POSTGRES_HOST = 'localhost'
const envs = {
    RABBITMQ_CONN_STRING: 'amqp://localhost',
    IMDB8_API_KEY: 'imdb8-api-key',
    MONGODB_CONN_STRING: 'mongodb://127.0.0.1:27017/db-test',
    POSTGRES_CONN_STRING: `postgresql://user:password123@${POSTGRES_HOST}:5432/dbtest`
}

const retrieveQueue = 'popcorn-planner.tvserie-retrieve'
const savedQueue = 'popcorn-planner.tvserie-saved'

tap.test('main', t => {
    let mongoDbClient, rabbitMqConnection, channel, postgresClient

    t.beforeEach(async () => {
        mongoDbClient = new MongoClient(envs.MONGODB_CONN_STRING, { useNewUrlParser: true, useUnifiedTopology: true });
        await mongoDbClient.connect()
        await mongoDbClient.db().dropDatabase()

        rabbitMqConnection = await amqp.connect(envs.RABBITMQ_CONN_STRING)
        channel = await rabbitMqConnection.createChannel()
        await channel.deleteQueue(retrieveQueue)

        postgresClient = knex({
            client: 'pg',
            connection: envs.POSTGRES_CONN_STRING
          });

        const hasTable = await postgresClient.schema.hasTable('episodes')
        if (hasTable){
            await postgresClient.schema.dropTable('episodes')
        }
    })

    t.afterEach(async () => {
        await channel.deleteQueue(retrieveQueue)
        rabbitMqConnection.close()

        await mongoDbClient.db().dropDatabase()
        await mongoDbClient.close()
        await postgresClient.destroy()
    })

    t.test('saves on mongo collection and postgres when a message is received', async t => {
        const mockAPI = mockIMDbAPI()

        channel.assertQueue(savedQueue, {
            durable: true
        });
        const savedCallbackMock = sinon.spy()
        channel.consume(savedQueue, savedCallbackMock, {noAck: true})
        
        const close = await main(logger, envs)
        await sendTestMessage(channel)

        await wait(1000)
        const allTvSeries = await mongoDbClient.db().collection('tvseries').find({}).toArray()

        t.equal(allTvSeries.length, 1)
        const addedTvSerie = allTvSeries[0]
        t.ok(addedTvSerie._id)
        t.ok(addedTvSerie.createdAt)
        t.ok(addedTvSerie.updatedAt)

        t.strictSame(omit(['_id', 'createdAt', 'updatedAt'], addedTvSerie), {
            serieId: 'tt0460681',
            title: 'Supernatural'
        })

        t.ok(savedCallbackMock.calledOnce)
        const {args} = savedCallbackMock.getCall(0)
        t.strictSame(args.length, 1)
        t.strictSame(JSON.parse(args[0].content.toString()), {title: 'Supernatural'})

        const episodes = await postgresClient.raw('SELECT * FROM episodes')
        t.strictSame(episodes.rows.length, 327)
        t.strictSame(episodes.rows.slice(0, 2), [{
            id: 1,
            episode: 1,
            season: 1,
            serieId: 'tt0460681'
        }, {
            id: 2,
            episode: 2,
            season: 1,
            serieId: 'tt0460681'
        }])

        await close()
        
        mockAPI.done()
        t.end()
    })
    t.end()
})

async function wait (time) { 
    return new Promise(resolve => setTimeout(resolve, time))
}

function mockIMDbAPI() {
    const scope = nock('https://imdb8.p.rapidapi.com')
        .get('/title/find')
        .query({
            q: 'Supernatural'
        })
        .reply(200, mockAPIFind)
        .get('/title/get-seasons')
        .query({
            tconst: 'tt0460681'
        })
        .reply(200, mockAPIEpisodes)
  return scope
}


async function sendTestMessage (channel) {
    const msg = JSON.stringify({name: 'Supernatural'})
    channel.assertQueue(retrieveQueue, {
        durable: true
    })
    channel.sendToQueue(retrieveQueue, Buffer.from(msg), {
        persistent: true
    })
}
