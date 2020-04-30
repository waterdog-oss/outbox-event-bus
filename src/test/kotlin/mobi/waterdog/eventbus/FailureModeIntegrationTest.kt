package mobi.waterdog.eventbus

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mobi.waterdog.eventbus.containers.PostgreSQLTestContainer
import mobi.waterdog.eventbus.containers.ProxiedKafkaTestContainer
import mobi.waterdog.eventbus.containers.ToxiProxy
import mobi.waterdog.eventbus.model.EventInput
import mobi.waterdog.eventbus.persistence.sql.DatabaseConnection
import mobi.waterdog.eventbus.persistence.sql.EventTable
import org.amshove.kluent.`should be greater than`
import org.amshove.kluent.`should equal`
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.select
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.koin.core.context.startKoin
import org.koin.core.context.stopKoin
import org.koin.core.inject
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FailureModeIntegrationTest : BaseIntegrationTest() {

    private val networkProxy = ToxiProxy.instance.getProxy(
        ProxiedKafkaTestContainer.instance,
        9093
    )
    override var servers = "PLAINTEXT://${networkProxy.containerIpAddress}:${networkProxy.proxyPort}"

    @BeforeAll
    fun initContext() {
        println("ORIGINAL: ${ProxiedKafkaTestContainer.instance.bootstrapServers}")
        println("ORIGINAL PORT: ${ProxiedKafkaTestContainer.instance.bootstrapServers.substringAfterLast(":")}")
        println("SERVERS: ${servers}")
        while (!PostgreSQLTestContainer.instance.isRunning) {
            runBlocking { delay(100) }
        }

        startKoin {
            modules(integrationTestModules)
        }

        while (!ProxiedKafkaTestContainer.instance.isRunning) {
            runBlocking { delay(100) }
        }
    }

    @AfterAll
    fun stopContext() {
        stopKoin()
        ToxiProxy.instance.stop()
        ProxiedKafkaTestContainer.instance.stop()
        PostgreSQLTestContainer.instance.stop()
    }

    @AfterEach
    fun cleanEventSenders() {
        val ebf: EventBusFactory by inject()
        ebf.shutdown()
    }

    @Test
    fun `Test that no events are lost if we lose connection to the engine - Kafka`() {
        // Given a producer sending messages
        val stopProducer = AtomicBoolean(false)
        val topic = UUID.randomUUID().toString()
        thread {
            val producer = createProducer(3600)
            while (!stopProducer.get()) {
                producer.send(
                    EventInput(
                        topic,
                        "message",
                        "application/json",
                        """ {"ts": ${Instant.now()}}""".toByteArray()
                    )
                )
                runBlocking {
                    delay(100)
                }
            }
        }

        // And: it runs for a while:
        runBlocking { delay(5000) }

        // When: The connection to kafka dies
        networkProxy.setConnectionCut(true)
        runBlocking { delay(10000) }

        // Then: there is a backlog of unsent messages
        stopProducer.set(true)
        val dbc: DatabaseConnection by inject()

        val totalByTopic = dbc.query { EventTable.select { EventTable.topic eq topic }.count() }
        val totalDeliveredByTopic =
            dbc.query { EventTable.select { (EventTable.topic eq topic) and (EventTable.delivered eq true) }.count() }
        totalByTopic `should be greater than` totalDeliveredByTopic
        println("TOTAL: $totalByTopic / TOTAL DELIVERED: $totalDeliveredByTopic")

        // And: when the message bus goes back up after a while
        runBlocking { delay(1000) }
        networkProxy.setConnectionCut(false)

        // Expect: all messages to be delivered after a while
        runBlocking { delay(10000) }
        val finalDeliveredByTopic =
            dbc.query { EventTable.select { (EventTable.topic eq topic) and (EventTable.delivered eq true) }.count() }
        totalByTopic `should equal` finalDeliveredByTopic
    }
}