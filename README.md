![CI](https://github.com/waterdog-oss/outbox-event-bus/workflows/CI/badge.svg)

# Outbox event bus
Kotlin based event bus that implements an outbox pattern approach to publishing domain level events in the context of database transactions. 
This allows to atomically update the database and publish events. Check [this](https://microservices.io/patterns/data/transactional-outbox.html) 
for an in-depth explanation of this pattern.

This solution uses a polling publisher to push messages to the event queue.

## What you're getting

This library targets scenarios where we want to reliably persist data and send events to a message bus.

### Concepts

* EventBusProvider: Main entry point used by producers and consumers allowing users to setup kafka.
* EventProducer: Sends events to the database, that will be eventually sent to the message queue;
* EventConsumer: Interface that provides a reactive stream of events that the user can subscribe to;
* Engine: The underlying messaging technology used to propagate the messages;
* LocalEventStore: Local database table that stores the events;

### Event structure

This library has an opinionated view about the internal structure of an event, this may change in the future, but for now
the events have the following structure (see the EventOutput class):

| Field     | Data type |
|-----------|-----------|
| uuid      | String    |
| timestamp | Instant   |
| topic     | String    |
| msgType   | String    |
| mimeType  | String    |
| payload   | ByteArray |

The payload is the user-specific content of the message. For now messages are encoded as JSON, but this may also change
in the future.

### Database table structure

| Column           | Data type    |
|------------------|--------------|
|topic             | varchar(255) |
| delivered        | boolean      |
| uuid             | char(36)     |
| stored_timestamp | datetime     |
| send_timestamp   | datetime     |
| msg_type         | varchar(255) |
| mime_type        | varchar(255) |
| payload          | blob |

### Configuration options

* _consumer.streamMode_: Defines the behavior for sending message acknowledge. Possible options are: 
    * AutoCommit: acknowledges reads automatically every _auto.commit.interval.ms_ millis;
    * EndOfBatchCommit: acknowledges reads once every message in the batch of messages that were retrieved are dealt with
    * MessageCommit: acknowledges every message individually
* _consumer.syncInterval_: Number of millis between message queue polls
* _consumer.backpressureStrategy_: How the reactive stream deals with backpressure. See: io.reactivex.BackpressureStrategy
* _producer.event.cleanup.intervalInSeconds_: How many seconds an event is retained after itn has been published. Default 7 days
### Failure model

The event relay will keep on trying to send events that fails delivery, while also writing a messasge with alert level ERROR indicating
that the failure has occurred.


### Caveats

Currently, we are using Exposed as our ORM, this forces the users of this library to also use exposed in order to have the
transactional properties that are implied.

## Getting started

* Checkout the project
* Run `gradlew clean build`
* Hack away :)

## Usage in your project

* Add the dependency: com.github.waterdog-oss: outbox-event-bus:<release-version>

### Setting up a producer:
1. Setup the dependencies (database and local event store)
2. Setup Kafka
3. Use the event bus provider, to get a producer
4. Send a message

Example:
```kotlin
// imports are omitted. Check the examples section - producer

// Step 1
val dataSource:DataSource = HikariDataSource(HikariConfig().apply {
    driverClassName = "org.h2.Driver"
    jdbcUrl = "jdbc:h2:mem:test"
    maximumPoolSize = 5
    isAutoCommit = false
    transactionIsolation = "TRANSACTION_REPEATABLE_READ"
    validate()
})

// See the producer example (mobi.waterdog.eventbus.example.sql.LocalEventStoreSql)
val localEventStore: LocalEventStore =  MyLocalEventStore()

// Step 2
val props = Properties()
//General cluster settings and config
props["bootstrap.servers"] = kafkaServer
//Kafka serialization config
props["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
props["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"

// Step 3
val ebp = EventBusProvider()
ebf.setupProducer(EventBackend.Kafka, localEventStore)
val producer = ebf.getProducer(props)

// Step 4
producer.send(EventInput("test", "OK", "text/plain", "sent at: ${Instant.now()}".toByteArray()))
```

### Setting up a consumer: 
1. Setup the EventBusProvider
2. Setup the kafka consumer
4. Setup a subscription to the stream

```kotlin
// imports are omitted. Check the examples section - consumer

// Step 1
val ebp = EventBusProvider()
ebp.setupConsumer(EventBackend.Kafka)

// Step 2
val props = Properties()
//General cluster settings and config
props["bootstrap.servers"] = kafkaServer
props["enable.auto.commit"] = "true"
props["auto.commit.interval.ms"] = "1000"
props["heartbeat.interval.ms"] = "3000"
props["session.timeout.ms"] = "10000"
props["auto.offset.reset"] = "latest"
//Kafka serialization config
props["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
props["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
//Event bus property that controls the sync loop and the auto-commit mode
props["consumer.syncInterval"] = "1000"
props["consumer.streamMode"] = "AutoCommit"
val consumer = ebf.getConsumer(props)

// Step 3
val topic="my-topic"
val consumerId = "consumer-group-id"
consumer.stream(topic, consumerId)
        .doOnError { it.printStackTrace() }
        .onErrorReturnItem(EventOutput.buildError())
        .subscribe {
           log.info("EVENT: ${it.uuid} @ ${it.timestamp}")
        }
```

## Metrics

The following metrics are exported (using [micrometer](https://micrometer.io)):

* events.store.timer - Accounts the time it takes to store an event in the event store
* events.store.error - Counts the number of errors storing events in the event store
* events.send.timer - Accounts the time it takes to send an event to the message backend
* events.send.error - Counts the number of errors sending errors to the message backend
* events.cleanup.timer - Accounts the time it takes to clear an event from the store
* events.cleanup.error - Counts the number of errors clearing events from the store

### Providing 

```kotlin
// Given: A meter registry
val meterRegistry = SimpleMeterRegistry()
meterRegistry.config().commonTags("service-name", "my-beautiful-service")

// See the producer example (mobi.waterdog.eventbus.example.sql.LocalEventStoreSql)
val localEventStore: LocalEventStore =  MyLocalEventStore()

val ebf = EventBusProvider(EventBackend.Kafka, meterRegistry)
ebf.setupProducer(localEventStore)

// You are then able to access your metrics
val timer = meterRegistry.get("events.store.timer").timer()
```

## Roadmap
* Separate event relay thread from producer
* Use toxiproxy to assert the correct when errors happen
* Add support for sending messages with keys!
* We are using JSON as a serialization mechanism. Allow the operators to provide their own
serialization mechanism so that other formats like protobufs can be used;
* Cleanup events that have been processed and are older than a given threshold;


