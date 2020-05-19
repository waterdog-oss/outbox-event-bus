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

    private
    val subscribers: MutableMap<String, ConcurrentHashMap<String, ConsumerStatus>> =
        ConcurrentHashMap()

    override fun stream(topicName: String, consumerId: String): Flowable<EventOutput> {
        val consumerStatus = subscribe(topicName, consumerId)
        return when (streamMode) {
            StreamMode.AutoCommit -> AutoCommitStreamStrategy(props).stream(
                consumerStatus.consumer,
                syncInterval,
                consumerStatus.isActive,
                backpressureStrategy
            )
            StreamMode.EndOfBatchCommit -> BatchStreamStrategy(props).stream(
                consumerStatus.consumer,
                syncInterval,
                consumerStatus.isActive,
                backpressureStrategy
            )
            StreamMode.MessageCommit -> MessageStreamStrategy(props).stream(
                consumerStatus.consumer,
                syncInterval,
                consumerStatus.isActive,
                backpressureStrategy
            )
        }
    }

    override fun unsubscribe(topicName: String, consumerId: String) {
        val consumerStatus = subscribers[topicName]?.remove(consumerId)
        if (consumerStatus != null) {
            consumerStatus.isActive.set(false)
            consumerStatus.consumer.wakeup()
        }
    }

    override fun listSubscribers(topicName: String): List<String> {
        return when (subscribers[topicName]) {
            null -> listOf()
            else -> subscribers[topicName]!!.keys().toList()
        }
    }

    private fun subscribe(topicName: String, subscriberId: String): ConsumerStatus {
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
            subscribers[topicName]!![subscriberId] = ConsumerStatus(
                isActive = AtomicBoolean(true),
                consumer = consumer
            )
        }
        return subscribers[topicName]!![subscriberId]!!
    }
}

private data class ConsumerStatus(val isActive: AtomicBoolean, val consumer: Consumer<String, String>)