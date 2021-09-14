# TvSeries Scraper

This is a component of the TvSeries Scraper application.

This service aims to populate a "tvseries" collection with the information about the Tv Series it's required. 

It listens on a queue on RabbitMQ, when a message with the name of a Tv Serie is received, it search for information about that serie using the IMDb Api. Once the information has been found, it updates a collection on mongodb.

Example of message received on RabbitMQ.

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