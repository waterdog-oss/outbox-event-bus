package mobi.waterdog.eventbus.example.consume
r
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mobi.waterdog.eventbus.model.EventOutput
import mobi.waterdog.eventbus.model.StreamMode
import mobi.waterdog.eventbus.model.EventBackend
import org.koin.core.KoinComponent
import org.slf4j.LoggerFactory
import java.util.Properties
import java.util.Random
import java.util.concurrent.Executors

object BaseProps {
    fun getDefault(kafkaServer: String): Properties {
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
        return props
    }
}

interface EventBusConsumer : Runnable {
    val consumer: EventConsumer
    val topic: String
    val consumerId: String
}

class ConsumerLoop(
    kafkaServer: String,
    override val topic: String,
    override val consumerId: String,
    offsetReset: String,
    streamMode: StreamMode = StreamMode.AutoCommit
) : KoinComponent, EventBusConsumer {

    companion object {
        private val log = LoggerFactory.getLogger(ConsumerLoop::class.java)
    }

    override val consumer: EventConsumer

    init {
        val props = BaseProps.getDefault(kafkaServer)
        props.setProperty("auto.offset.reset", offsetReset)
        props.setProperty("consumer.streamMode", streamMode.name)
        if (streamMode in setOf(StreamMode.EndOfBatchCommit, StreamMode.MessageCommit)) {
            props.setProperty("enable.auto.commit", "false")
        }

        val ebp = EventBusProvider(EventBackend.Kafka)
        consumer = ebp.getConsumer(props)
    }

    override fun run() {
        consumer.stream(topic, consumerId)
            .doOnError { it.printStackTrace() }
            .onErrorReturnItem(EventOutput.buildError())
            .subscribe {
                log.info("EVENT: ${it.uuid} @ ${it.timestamp}")
            }
    }
}

class CustomConfigConsumerLoop(
    override val topic: String,
    override val consumerId: String,
    props: Properties
) : KoinComponent, EventBusConsumer {

    companion object {
        private val log = LoggerFactory.getLogger(ConsumerLoop::class.java)
    }

    override val consumer: EventConsumer

    init {
        val ebp = EventBusProvider(EventBackend.Kafka)
        consumer = ebp.getConsumer(props)
    }

    override fun run() {
        consumer.stream(topic, consumerId)
            .doOnError { it.printStackTrace() }
            .onErrorReturnItem(EventOutput.buildError())
            .subscribe {
                log.info("EVENT: ${it.uuid} @ ${it.timestamp}")
            }
    }
}

class ByzantineConsumer(
    kafkaServer: String,
    override val topic: String,
    override val consumerId: String,
    offsetReset: String,
    streamMode: StreamMode = StreamMode.AutoCommit
) : KoinComponent, EventBusConsumer {

    companion object {
        private val log = LoggerFactory.getLogger(ByzantineConsumer::class.java)
    }

    override val consumer: EventConsumer

    init {
        val props = BaseProps.getDefault(kafkaServer)
        props.setProperty("auto.offset.reset", offsetReset)
        props.setProperty("consumer.streamMode", streamMode.name)

        val ebp = EventBusProvider(EventBackend.Kafka)
        consumer = ebp.getConsumer(props)
    }

    override fun run() {
        consumer.stream(topic, consumerId)
            .doOnError { it.printStackTrace() }
            .onErrorReturnItem(EventOutput.buildError())
            .subscribe {
                try {
                    val errorChance = Random().nextDouble()
                    if (errorChance < 0.5) {
                        log.info("EVENT: ${it.uuid} @ ${it.timestamp}")
                    } else {
                        throw RuntimeException("Boom!")
                    }
                } catch (rte: java.lang.RuntimeException) {
                    log.error("Byzantine consumer stuff...")
                }
            }
    }
}

fun main() {
    val server = "kafka-service:9092"
    val topic = "test-0.10"
    val random = Random().nextLong()
    println("STARTING: $server")

    testConfigErrors(server, random)

    val consumers: List<EventBusConsumer> = listOf(
        //Auto-commit
        ConsumerLoop(server, topic, "start-on-latest-$random", "latest"),
        ConsumerLoop(server, topic, "start-on-earliest-$random", "earliest"),
        ByzantineConsumer(server, topic, "byzantine-$random", "earliest"),
        //Batch commit
        ConsumerLoop(server, topic, "batch-commit-$random", "earliest", StreamMode.EndOfBatchCommit),
        ByzantineConsumer(server, topic, "batch-byzantine-$random", "earliest", StreamMode.EndOfBatchCommit),
        //Message commit
        ConsumerLoop(server, topic, "message-commit-$random", "earliest", StreamMode.MessageCommit),
        ByzantineConsumer(server, topic, "message-byzantine-$random", "earliest", StreamMode.MessageCommit)
    )

    val executor = Executors.newFixedThreadPool(consumers.size)
    consumers.forEach { executor.submit(it) }

    println("RUNNING")
    var count = 0
    val limit = 60
    while (count <= limit) {
        count++
        println("RAN FOR $count of $limit")
        runBlocking { delay(1000) }
    }

    println("Shutting down...")
    consumers.forEach { it.consumer.unsubscribe(it.topic, it.consumerId) }
    println("Bye")
}

private fun testConfigErrors(server: String, random: Long) {
    try {
        val badProps = BaseProps.getDefault(server)
        badProps.remove("enable.auto.commit")
        val consumer = CustomConfigConsumerLoop("test", "bad-props-no-autocommit-$random", badProps)
        consumer.run()
        throw RuntimeException("INVALID CONFIG")
    } catch (ex: IllegalArgumentException) {
        println("Handled expected illegal arg: ${ex.message}")
    }

    try {
        val badProps = BaseProps.getDefault(server)
        badProps["consumer.streamMode"] = "EndOfBatchCommit"
        val consumer = CustomConfigConsumerLoop("test", "bad-props-autocommit-enabled-$random", badProps)
        consumer.run()
        throw RuntimeException("INVALID CONFIG")
    } catch (ex: IllegalArgumentException) {
        println("Handled expected illegal arg: ${ex.message}")
    }

    try {
        val badProps = BaseProps.getDefault(server)
        badProps["consumer.streamMode"] = "MessageCommit"
        val consumer = CustomConfigConsumerLoop("test", "bad-props-autocommit-enabled-$random", badProps)
        consumer.run()
        throw RuntimeException("INVALID CONFIG")
    } catch (ex: IllegalArgumentException) {
        println("Handled expected illegal arg: ${ex.message}")
    }
}