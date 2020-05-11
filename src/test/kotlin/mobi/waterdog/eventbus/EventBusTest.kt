package mobi.waterdog.eventbus

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mobi.waterdog.eventbus.model.EventInput
import mobi.waterdog.eventbus.sql.DatabaseConnection
import mobi.waterdog.eventbus.sql.EventTable
import mobi.waterdog.eventbus.sql.LocalEventStoreSql
import org.amshove.kluent.`should be true`
import org.amshove.kluent.`should equal`
import org.jetbrains.exposed.sql.SchemaUtils
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.koin.test.KoinTest

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EventBusTest : KoinTest {

    @AfterAll
    fun afterAll() {
        ebp.shutdown()
        dbc.query { SchemaUtils.drop(EventTable) }
    }

    private val targetTopic1 = "MyTopic1"
    private val targetTopic2 = "MyTopic2"
    private val ebp = EventBusProvider(EventBackend.Local)
    private val dataSource = HikariDataSource(HikariConfig().apply {
        driverClassName = "org.h2.Driver"
        jdbcUrl = "jdbc:h2:mem:test"
        maximumPoolSize = 5
        isAutoCommit = false
        transactionIsolation = "TRANSACTION_REPEATABLE_READ"
        validate()
    })
    private val dbc = DatabaseConnection(dataSource)
    private val localEventStore by lazy {

        dbc.query { SchemaUtils.create(EventTable) }
        LocalEventStoreSql(dbc)
    }
    private val eventProducer: EventProducer by lazy {
        ebp.setupProducer(localEventStore)
        ebp.getProducer()
    }
    private val eventConsumer: EventConsumer by lazy { ebp.getConsumer() }

    @AfterEach
    fun cleanup() {

        listOf(targetTopic1, targetTopic2).forEach { topic ->
            eventConsumer.listSubscribers(topic).forEach { consumerId ->
                eventConsumer.unsubscribe(topic, consumerId)
            }

            eventConsumer.listSubscribers(topic).size `should equal` 0
        }
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
            eventProducer.send(event)
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
}