# TvSeries Scraper

This is a component of the PopCorn Planner application.

This service aims to populate a "tvseries" collection with the information about the Tv Series it's required.

It listens on a queue on RabbitMQ when a message with the name of a Tv Serie is received, it searches for information about that series using the IMDb API. Once the information has been found, it updates a collection on MongoDB.

Example of a message received on RabbitMQ.

```json
{ "name": "Supernatural" }
```

Example of response from the IMDb API: look at "find-title.example.json" file.

Example of document saved on MongoDB:

```json
{
    "serieId":"tt0460681",
    "createdAt":"1631644913465",
    "numberOfEpisodes": "327",
    "title":"Supernatural",
    "updatedAt": "1631644913465"
}
```

## Local Testing

Install the dependencies

```
npm ci
```

Run the docker images of MongoDB and RabbitMQ

```
docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.9
docker run -it --rm --name mongo4.4 -p 27017:27017 mongo:4.4
```

Run the tests

```
npm t
```
