'use strict'

const tap = require('tap')
const {MongoClient} = require('mongodb')
const amqp = require('amqplib')
const logger = require('pino')({level: 'silent'})
const nock = require('nock')
const {omit} = require('ramda')
const sinon = require('sinon')

const mockAPI = require('./find-title.example.json')

const main = require('./index')

const envs = {
    RABBITMQ_CONN_STRING: 'amqp://localhost',
    IMDB8_API_KEY: 'imdb8-api-key',
    MONGODB_CONN_STRING: 'mongodb://127.0.0.1:27017/db-test'
}

const retrieveQueue = 'popcorn-planner.tvserie-retrieve'
const savedQueue = 'popcorn-planner.tvserie-saved'

tap.test('main', t => {
    let mongoDbClient, rabbitMqConnection, channel

    t.beforeEach(async () => {
        mongoDbClient = new MongoClient(envs.MONGODB_CONN_STRING, { useNewUrlParser: true, useUnifiedTopology: true });
        await mongoDbClient.connect()
        await mongoDbClient.db().dropDatabase()

        rabbitMqConnection = await amqp.connect(envs.RABBITMQ_CONN_STRING)
        channel = await rabbitMqConnection.createChannel()
        await channel.deleteQueue(retrieveQueue)
    })

    t.afterEach(async () => {
        await channel.deleteQueue(retrieveQueue)
        rabbitMqConnection.close()

        await mongoDbClient.db().dropDatabase()
        await mongoDbClient.close()
    })

    t.test('saves on mongo collection when a message is received', async t => {
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
            numberOfEpisodes: 327,
            title: 'Supernatural'
        })

        t.ok(savedCallbackMock.calledOnce)
        const {args} = savedCallbackMock.getCall(0)
        t.strictSame(args.length, 1)
        t.strictSame(JSON.parse(args[0].content.toString()), {title: 'Supernatural'})
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
        .reply(200, mockAPI)
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
