package mobi.waterdog.eventbus

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import mobi.waterdog.eventbus.containers.KafkaTestContainer
import mobi.waterdog.eventbus.containers.PostgreSQLTestContainer
import mobi.waterdog.eventbus.example.app.LineItem
import mobi.waterdog.eventbus.example.app.Order
import mobi.waterdog.eventbus.example.app.OrderService
import mobi.waterdog.eventbus.example.app.OrderTable
import mobi.waterdog.eventbus.model.EventInput
import mobi.waterdog.eventbus.model.EventOutput
import mobi.waterdog.eventbus.persistence.sql.DatabaseConnection
import mobi.waterdog.eventbus.persistence.sql.EventTable
import org.amshove.kluent.`should be null`
import org.amshove.kluent.`should equal`
import org.amshove.kluent.`should not be null`
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.awaitility.Awaitility.await
import org.jetbrains.exposed.sql.selectAll
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.koin.core.context.startKoin
import org.koin.core.context.stopKoin
import org.koin.core.inject
import org.koin.dsl.module
import org.koin.test.KoinTest
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Instant
import java.util.Properties
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource
import kotlin.concurrent.thread

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaIntegrationTest : KoinTest {

    @BeforeAll
    fun initContext() {
        startKoin {
            modules(
                listOf(
                    module {
                        single<DataSource> {
                            val config = HikariConfig()
                            config.driverClassName = PostgreSQLTestContainer.instance.driverClassName
                            config.jdbcUrl = PostgreSQLTestContainer.instance.jdbcUrl
                            config.username = PostgreSQLTestContainer.instance.username
                            config.password = PostgreSQLTestContainer.instance.password
                            config.maximumPoolSize = 10
                            config.isAutoCommit = false
                            config.transactionIsolation = "TRANSACTION_REPEATABLE_READ"
                            config.leakDetectionThreshold = 10000
                            config.poolName = "tests"
                            config.validate()

                            HikariDataSource(config)
                        }
                    },
                    databaseConnectionModule(),
                    eventBusKoinModule()
                )
            )
        }
    }

    @AfterAll
    fun stopContext() {
        val ebf: EventBusFactory by inject()
        ebf.shutdown()
        stopKoin()
    }

    @Nested
    inner class NonTransactionalIntegrationTest {

        @ParameterizedTest
        @CsvSource("1,1", "1,5", "5,1", "5,5")
        fun `Many consumers and many producers`(numConsumers: Int, numProducers: Int) {
            // Given: number of concurrent consumers
            val received = ConcurrentHashMap<String, Int>()
            val topic = UUID.randomUUID().toString()
            repeat(numConsumers) {
                createConsumerThread(topic) { consumerGroupId, count: Int ->
                    received[consumerGroupId] = count
                }
            }

            // When: there are different numbers of producers sending messages
            val numMessagesPerProducer = 100
            repeat(numProducers) {
                createProducerThread(topic, numMessagesPerProducer)
            }

            // Then: The overall number of received messages is correct
            await()
                .atMost(30, TimeUnit.SECONDS)
                .until {
                    received.isNotEmpty() &&
                        received.map { it.value }
                            .reduce { acc, value -> acc + value } == (numProducers * numConsumers * numMessagesPerProducer)
                }
        }
    }

    @Nested
    inner class TransactionalIntegrationTest {

        @Test
        fun `Sends an event after some changes have been committed to the database`() {
            // Given: a consumer thread
            val received = ConcurrentHashMap<String, Int>()
            val topic = UUID.randomUUID().toString()
            createConsumerThread(topic) { consumerGroupId, count: Int ->
                received[consumerGroupId] = count
            }

            // And: a block of code that sends an event after a complex insert into a database
            val orderServiceEventProducer = createProducer()
            val service = OrderService(topic, orderServiceEventProducer)
            val newOrder = service.createOrder("ACME Inc.", listOf(LineItem("X", 1)))

            // Then: The expected item is created
            service.getOrderById(newOrder.id).`should not be null`()

            // And: the event is received
            await()
                .atMost(30, TimeUnit.SECONDS)
                .until {
                    received.isNotEmpty() &&
                        received.map { it.value }
                            .reduce { acc, value -> acc + value } == 1
                }
        }

        @Test
        fun `Exceptions are correctly handled when the event is the last thing to be committed`() {
            // Given: a consumer thread
            val received = AtomicBoolean(false)
            val topic = UUID.randomUUID().toString()
            createConsumerThread(topic) { _, _ ->
                received.set(true)
            }

            // And the old count of events in the database
            val dbc: DatabaseConnection by inject()
            val oldCount = dbc.query {
                EventTable.selectAll().count()
            }

            // And: a block of code that throws an exception due to bad data
            var newOrder: Order? = null
            val orderServiceEventProducer = createProducer()
            assertThrows<IllegalArgumentException> {
                val service = OrderService(topic, orderServiceEventProducer)
                newOrder = service.createOrder("ACME Inc.", listOf(LineItem("X", 0)))
            }

            // Then: The expected item is created
            newOrder.`should be null`()

            // And: the number of events remains the same
            val updatedCount = dbc.query {
                EventTable.selectAll().count()
            }
            updatedCount `should equal` oldCount
        }

        @Test
        fun `Exceptions are correctly handled when the event is NOT the last thing to be committed`() {
            // Given: a consumer thread
            val received = AtomicBoolean(false)
            val topic = UUID.randomUUID().toString()
            createConsumerThread(topic) { _, _ ->
                received.set(true)
            }
            val orderServiceEventProducer = createProducer()
            val service = OrderService(topic, orderServiceEventProducer)

            // And the old count of events in the database
            val dbc: DatabaseConnection by inject()
            val oldCount = dbc.query {
                EventTable.selectAll().count()
            }

            val oldOrders = dbc.query {
                OrderTable.selectAll().count()
            }

            // And: a block of code that throws an exception due to bad data
            var newOrder: Order? = null
            assertThrows<IllegalArgumentException> {
                newOrder = service.createOrder(
                    "ACME Inc.", listOf(
                        LineItem("X", 1),
                        LineItem("Y", 0)
                    ), isEvil = true
                )
            }

            // Then: The expected item is created
            newOrder.`should be null`()
            val updatedCount = dbc.query {
                EventTable.selectAll().count()
            }

            val updateOrders = dbc.query {
                OrderTable.selectAll().count()
            }
            updateOrders `should equal` oldOrders
            updatedCount `should equal` oldCount
        }
    }

    private fun createConsumerThread(
        topic: String,
        consumerGroup: String = UUID.randomUUID().toString(),
        handler: (consumerGroup: String, numReceived: Int) -> Unit
    ) {
        val isReady = AtomicBoolean(false)
        val numReceived = AtomicInteger(0)
        thread {
            val stream = createConsumer().stream(topic, consumerGroup)

            stream.doOnError { it.printStackTrace() }
                .onErrorReturnItem(EventOutput.buildError())
                .doOnSubscribe {
                    isReady.set(true)
                    println("ON SUBSCRIBE!!!!")
                }
                .subscribe {
                    println("GOT EVENT")
                    handler(consumerGroup, numReceived.incrementAndGet())
                }
        }

        await()
            .atMost(60, TimeUnit.SECONDS)
            .untilTrue(isReady)
    }

    private fun createProducerThread(topic: String, numMessagesToSend: Int) {
        val isReady = AtomicBoolean(false)

        thread {
            val producer = createProducer()
            isReady.set(true)
            repeat(numMessagesToSend) { n ->
                val payload =
                    """{"timestamp": "${Instant.now()}", "producerId": "0", "messageId": "$n"}""".toByteArray()
                producer.send(EventInput(topic, "message", "application/json", payload))
            }
        }

        await()
            .atMost(10, TimeUnit.SECONDS)
            .untilTrue(isReady)
    }

    private fun createConsumer(): EventConsumer {
        val props = Properties()
        //General cluster settings and config
        props["bootstrap.servers"] = KafkaTestContainer.instance.bootstrapServers
        props["enable.auto.commit"] = "true"
        props["auto.commit.interval.ms"] = "1000"
        props["heartbeat.interval.ms"] = "3000"
        props["session.timeout.ms"] = "10000"
        props["auto.offset.reset"] = "earliest"
        //Kafka serialization config
        props["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        props["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        //Event bus property that controls the sync loop and the auto-commit mode
        props["consumer.syncInterval"] = "1000"
        props["consumer.streamMode"] = "AutoCommit"

        val ebf: EventBusFactory by inject()
        ebf.setup(EventBackend.Kafka)
        return ebf.getConsumer(props)
    }

    private fun createProducer(): EventProducer {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaTestContainer.instance.bootstrapServers
        props[ProducerConfig.CLIENT_ID_CONFIG] = UUID.randomUUID().toString()
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name

        val ebf: EventBusFactory by inject()
        ebf.setup(EventBackend.Kafka)
        return ebf.getProducer(props)
    }
}