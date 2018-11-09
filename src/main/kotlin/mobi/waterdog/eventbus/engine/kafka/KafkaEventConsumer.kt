package mobi.waterdog.eventbus.engine.kafka

import com.fasterxml.jackson.module.kotlin.readValue
import io.reactivex.BackpressureStrategy
import io.reactivex.Flowable
import io.reactivex.FlowableEmitter
import mobi.waterdog.eventbus.EventConsumer
import mobi.waterdog.eventbus.model.EventOutput
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

internal class KafkaEventConsumer(private val props: Properties) : EventConsumer {

    private val subscribers: MutableMap<String, ConcurrentHashMap<String, Consumer<String, String>>> =
        ConcurrentHashMap()

    override fun stream(topicName: String, consumerId: String): Flowable<EventOutput> {
        val consumer = subscribe(topicName, consumerId)
        return createFlowable(BackpressureStrategy.ERROR) { emitter ->
            while (true) {
                val syncInterval: Long =
                    if (props["consumer.syncInterval"] != null) props["consumer.syncInterval"].toString().toLong() else 100L
                val records = consumer.poll(Duration.ofMillis(syncInterval))
                for (record in records) {
                    val kafkaEvent: KafkaEvent = JsonSettings.mapper.readValue(record.value())
                    emitter.onNext(kafkaEvent.toEventOutput())
                }
            }
        }
    }

    override fun unsubscribe(topicName: String, consumerId: String) {
        subscribers[topicName]?.remove(consumerId)
    }

    override fun listSubscribers(topicName: String): List<String> {
        return when (subscribers[topicName]) {
            null -> listOf()
            else -> subscribers[topicName]!!.keys().toList()
        }
    }

    private fun createFlowable(strategy: BackpressureStrategy, onSubscribe: (FlowableEmitter<EventOutput>) -> Unit) =
        Flowable.create(onSubscribe, strategy)

    private fun subscribe(topicName: String, subscriberId: String): Consumer<String, String> {
        if (subscribers[topicName] == null) {
            subscribers[topicName] = ConcurrentHashMap()
        }

        if (subscribers[topicName]!![subscriberId] == null) {
            val consumer: Consumer<String, String> = KafkaConsumer(props)
            consumer.subscribe(listOf(topicName))
            subscribers[topicName]!![subscriberId] = consumer
        }
        return subscribers[topicName]!![subscriberId]!!
    }
}