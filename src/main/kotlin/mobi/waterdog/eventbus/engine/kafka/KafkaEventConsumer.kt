package mobi.waterdog.eventbus.engine.kafka

import io.reactivex.BackpressureStrategy
import io.reactivex.Flowable
import mobi.waterdog.eventbus.EventConsumer
import mobi.waterdog.eventbus.engine.kafka.modes.AutoCommitStreamStrategy
import mobi.waterdog.eventbus.engine.kafka.modes.BatchStreamStrategy
import mobi.waterdog.eventbus.engine.kafka.modes.MessageStreamStrategy
import mobi.waterdog.eventbus.model.EventOutput
import mobi.waterdog.eventbus.model.StreamMode
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

internal class KafkaEventConsumer(private val props: Properties) : EventConsumer {

    private val syncInterval = props["consumer.syncInterval"]?.toString()?.toLong() ?: 100L
    private val streamMode = props["consumer.streamMode"]?.let {
        StreamMode.valueOf(it.toString())
    } ?: StreamMode.AutoCommit
    private val backpressureStrategy = props["consumer.backpressureStrategy"]?.let {
        BackpressureStrategy.valueOf(it.toString())
    } ?: BackpressureStrategy.ERROR
    private val isPollLoopStarted = AtomicBoolean(true)

    private
    val subscribers: MutableMap<String, ConcurrentHashMap<String, Consumer<String, String>>> =
        ConcurrentHashMap()

    override fun stream(topicName: String, consumerId: String): Flowable<EventOutput> {
        val consumer = subscribe(topicName, consumerId)
        return when (streamMode) {
            StreamMode.AutoCommit -> AutoCommitStreamStrategy(props).stream(
                consumer,
                syncInterval,
                isPollLoopStarted,
                backpressureStrategy
            )
            StreamMode.EndOfBatchCommit -> BatchStreamStrategy(props).stream(
                consumer,
                syncInterval,
                isPollLoopStarted,
                backpressureStrategy
            )
            StreamMode.MessageCommit -> MessageStreamStrategy(props).stream(
                consumer,
                syncInterval,
                isPollLoopStarted,
                backpressureStrategy
            )
        }
    }

    override fun unsubscribe(topicName: String, consumerId: String) {
        isPollLoopStarted.set(false)
        val consumer = subscribers[topicName]?.remove(consumerId)
        consumer?.wakeup()
    }

    override fun listSubscribers(topicName: String): List<String> {
        return when (subscribers[topicName]) {
            null -> listOf()
            else -> subscribers[topicName]!!.keys().toList()
        }
    }

    private fun subscribe(topicName: String, subscriberId: String): Consumer<String, String> {
        if (subscribers[topicName] == null) {
            subscribers[topicName] = ConcurrentHashMap()
        }

        if (subscribers[topicName]!![subscriberId] == null) {
            val consumerProps = Properties()
            props.stringPropertyNames().forEach { propName ->
                consumerProps.setProperty(propName, props[propName].toString())
            }
            consumerProps.setProperty("group.id", subscriberId)
            val consumer: Consumer<String, String> = KafkaConsumer(consumerProps)
            consumer.subscribe(listOf(topicName))
            subscribers[topicName]!![subscriberId] = consumer
        }
        return subscribers[topicName]!![subscriberId]!!
    }
}