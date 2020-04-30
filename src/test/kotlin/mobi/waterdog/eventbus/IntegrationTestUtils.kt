package mobi.waterdog.eventbus

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import mobi.waterdog.eventbus.containers.KafkaTestContainer
import mobi.waterdog.eventbus.containers.PostgreSQLTestContainer
import mobi.waterdog.eventbus.model.EventInput
import mobi.waterdog.eventbus.model.EventOutput
import mobi.waterdog.eventbus.model.StreamMode
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.awaitility.Awaitility
import org.koin.core.inject
import org.koin.dsl.module
import org.koin.test.KoinTest
import java.time.Instant
import java.util.Properties
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource
import kotlin.concurrent.thread

val integrationTestModules = listOf(
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

open class BaseIntegrationTest : KoinTest {

    protected open val servers = KafkaTestContainer.instance.bootstrapServers!!

    protected fun createConsumerThread(
        topic: String,
        consumerGroup: String = UUID.randomUUID().toString(),
        streamMode: StreamMode = StreamMode.AutoCommit,
        handler: (consumerGroup: String, numReceived: Int) -> Unit
    ) {
        val isReady = AtomicBoolean(false)
        val numReceived = AtomicInteger(0)
        thread {
            val stream = createConsumer(streamMode).stream(topic, consumerGroup)

            stream.doOnError { it.printStackTrace() }
                .onErrorReturnItem(EventOutput.buildError())
                .doOnSubscribe {
                    isReady.set(true)
                }
                .subscribe {
                    handler(consumerGroup, numReceived.incrementAndGet())
                }
        }

        Awaitility.await()
            .atMost(60, TimeUnit.SECONDS)
            .untilTrue(isReady)
    }

    protected fun createProducerThread(topic: String, numMessagesToSend: Int) {
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

        Awaitility.await()
            .atMost(10, TimeUnit.SECONDS)
            .untilTrue(isReady)
    }

    protected fun createConsumer(streamMode: StreamMode): EventConsumer {
        val props = Properties()
        //General cluster settings and config
        props["bootstrap.servers"] = servers
        props["heartbeat.interval.ms"] = "3000"
        props["session.timeout.ms"] = "10000"
        props["auto.offset.reset"] = "earliest"
        props["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        props["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        //Event bus property that controls the sync loop and the auto-commit mode

        if (streamMode == StreamMode.AutoCommit) {
            props["enable.auto.commit"] = "true"
            props["auto.commit.interval.ms"] = "1000"
            props["consumer.syncInterval"] = "1000"
            props["consumer.streamMode"] = "AutoCommit"
        } else if (streamMode == StreamMode.EndOfBatchCommit) {
            props["enable.auto.commit"] = "false"
            props["consumer.streamMode"] = "EndOfBatchCommit"
        } else if (streamMode == StreamMode.MessageCommit) {
            props["enable.auto.commit"] = "false"
            props["consumer.streamMode"] = "MessageCommit"
        }
        val ebf: EventBusFactory by inject()
        ebf.setup(EventBackend.Kafka)
        return ebf.getConsumer(props)
    }

    protected fun createProducer(cleanupIntervalSeconds: Int = 0): EventProducer {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = servers
        props[ProducerConfig.CLIENT_ID_CONFIG] = UUID.randomUUID().toString()
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        if (cleanupIntervalSeconds > 0) {
            props["producer.event.cleanup.intervalInSeconds"] = "$cleanupIntervalSeconds"
        }

        val ebf: EventBusFactory by inject()
        ebf.setup(EventBackend.Kafka)
        return ebf.getProducer(props)
    }
}