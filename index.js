'use strict'

const amqp = require('amqplib')
var axios = require("axios").default;


require('dotenv').config()

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
    return {
        id,
        numberOfEpisodes,
        title
    }
}

async function run(){
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
              const msgObj = JSON.parse(msg.content.toString())
              console.log("[x] Received %s", msgObj)   
              getEpisodesByName(msgObj.name)
                .then(tvSerieDetail => {
                    console.log('tvSerieDetail ', tvSerieDetail)
                    channel.ack(msg)  
                })
    }, {
        noAck: false
    })
}

run()