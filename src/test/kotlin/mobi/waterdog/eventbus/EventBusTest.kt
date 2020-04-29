package mobi.waterdog.eventbus

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mobi.waterdog.eventbus.model.EventInput
import org.amshove.kluent.`should be true`
import org.amshove.kluent.`should equal`
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.koin.core.context.startKoin
import org.koin.core.context.stopKoin
import org.koin.dsl.module
import org.koin.test.KoinTest
import org.koin.test.inject
import javax.sql.DataSource

class EventBusTest : KoinTest {
    @Suppress("unused")
    @BeforeAll
    fun beforeClass() {
        startKoin {
            modules(
                listOf(
                    module {
                        single<DataSource> {
                            HikariDataSource(HikariConfig().apply {
                                driverClassName = "org.h2.Driver"
                                jdbcUrl = "jdbc:h2:mem:test"
                                maximumPoolSize = 5
                                isAutoCommit = false
                                transactionIsolation = "TRANSACTION_REPEATABLE_READ"
                                validate()
                            })
                        }
                    },
                    databaseConnectionModule(),
                    eventBusKoinModule()
                )
            )
        }
    }

    private val targetTopic1 = "MyTopic1"
    private val targetTopic2 = "MyTopic2"
    private val ebf by inject<EventBusFactory>()
    private val eventProducer: EventProducer by lazy { ebf.getProducer() }
    private val eventConsumer: EventConsumer by lazy { ebf.getConsumer() }

    @AfterEach
    fun cleanup() {

        listOf(targetTopic1, targetTopic2).forEach { topic ->
            eventConsumer.listSubscribers(topic).forEach { consumerId ->
                eventConsumer.unsubscribe(topic, consumerId)
            }

            eventConsumer.listSubscribers(topic).size `should equal` 0
        }
    }

    @BeforeEach
    fun setup() {
        ebf.setup()
    }

    @Test
    fun `The producer is able to send events to a topic`() {
        val jsonMsg = "{\"foo\":\"bar\"}"

        val n = 10
        eventConsumer.stream(targetTopic1).subscribe {
            String(it.payload) `should equal` jsonMsg
        }

        repeat(n) {
            val event = EventInput(
                targetTopic1,
                "Sample",
                "application/json",
                jsonMsg.toByteArray()
            )
            eventProducer.sendAsync(event)
        }
    }

    @Test
    fun `The producer is able to send and wait for ack events to a topic`() {
        val jsonMsg = "{\"foo\":\"bar\"}"
        var count = 0
        val n = 10
        eventConsumer.stream(targetTopic2)
            .filter { it.msgType == "Sample" && it.mimeType == "application/json" }
            .map { String(it.payload) }
            .subscribe {
                it `should equal` jsonMsg
                count += 1
            }
        eventConsumer.listSubscribers(targetTopic2).size `should equal` 1

        runBlocking {
            repeat(n) {
                val event = EventInput(
                    targetTopic2,
                    "Sample",
                    "application/json",
                    jsonMsg.toByteArray()
                )
                val result = eventProducer.send(event)
                result.`should be true`()
            }
            delay(500)
        }

        count `should equal` n
    }

    @Suppress("unused")
    @AfterAll
    fun afterAll() {
        stopKoin()
    }
}