package mobi.waterdog.eventbus

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import mobi.waterdog.eventbus.containers.KafkaTestContainer
import mobi.waterdog.eventbus.containers.PostgreSQLTestContainer
import mobi.waterdog.eventbus.model.EventInput
import mobi.waterdog.eventbus.model.EventOutput
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.koin.core.context.startKoin
import org.koin.core.context.stopKoin
import org.koin.core.inject
import org.koin.core.logger.Level
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
        println(PostgreSQLTestContainer.instance.containerId)
        println(KafkaTestContainer.instance.containerId)
        println("Init Koin")

        startKoin {
            printLogger(Level.DEBUG)
            modules(
                listOf(
                    module {
                        single<DataSource> {
                            val config = HikariConfig()
                            config.driverClassName = "org.postgresql.Driver"
                            config.jdbcUrl = PostgreSQLTestContainer.instance.jdbcUrl
                            config.username = PostgreSQLTestContainer.instance.username
                            config.password = PostgreSQLTestContainer.instance.password
                            config.maximumPoolSize = 10
                            config.isAutoCommit = true
                            config.transactionIsolation = "TRANSACTION_REPEATABLE_READ"
                            config.leakDetectionThreshold = 10000
                            config.poolName = "tests"
                            config.validate()

                            HikariDataSource(config)
                        }
                    },
                    eventBusKoinModule()
                )
            )
        }
    }

    @AfterAll
    fun stopContext() {
        stopKoin()
    }

    @Nested
    inner class NonTransactionalIntegrationTest {

        @ParameterizedTest
        @CsvSource("1,1", "1,5", "5,1", "5,5")
        fun `Many producers many consumers`(numConsumers: Int, numProducers: Int) {
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

        private fun createConsumerThread(
            topic: String,
            consumerGroup: String = UUID.randomUUID().toString(),
            handler: (consumerGroup: String, numReceived: Int) -> Unit
        ) {
            val isReady = AtomicBoolean(false)
            val numReceived = AtomicInteger(0)
            thread {
                val consumer = createConsumer()

                consumer.stream(topic, consumerGroup)
                    .doOnError { it.printStackTrace() }
                    .onErrorReturnItem(EventOutput.buildError())
                    .doOnSubscribe {
                        isReady.set(true)
                        println("Consumer: $consumerGroup ready")
                    }
                    .subscribe {
                        println("Receiving")
                        handler(consumerGroup, numReceived.incrementAndGet())
                    }
            }

            await()
                .atMost(10, TimeUnit.SECONDS)
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
                    println("SENDING: $topic / $n")
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
}