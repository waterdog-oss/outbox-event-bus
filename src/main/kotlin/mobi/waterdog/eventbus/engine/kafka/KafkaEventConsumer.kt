package mobi.waterdog.eventbus.engine.kafka

import com.fasterxml.jackson.module.kotlin.readValue
import io.reactivex.BackpressureStrategy
import io.reactivex.Flowable
import io.reactivex.FlowableEmitter
import mobi.waterdog.eventbus.EventConsumer
import mobi.waterdog.eventbus.model.EventOutput
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

internal class KafkaEventConsumer(private val props: Properties) : EventConsumer {

    companion object {
        private val log = LoggerFactory.getLogger(KafkaEventConsumer::class.java)
    }

    private val syncInterval = props["consumer.syncInterval"]?.toString()?.toLong() ?: 100L
    private val backpressureStrategy =
        props["consumer.backpressureStrategy"]?.let { BackpressureStrategy.valueOf(it.toString()) }
            ?: BackpressureStrategy.ERROR

    private
    val subscribers: MutableMap<String, ConcurrentHashMap<String, Consumer<String, String>>> =
        ConcurrentHashMap()

    override fun stream(topicName: String, consumerId: String): Flowable<EventOutput> {
        val consumer = subscribe(topicName, consumerId)
        return createFlowable(backpressureStrategy) { emitter ->
            while (true) {
                val records = consumer.poll(Duration.ofMillis(syncInterval))
                for (record in records) {
                    consumeRecord(record, emitter)
                }
                commitConsumedOffsets(consumer, emitter)
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

    private fun commitConsumedOffsets(
        consumer: Consumer<String, String>,
        emitter: FlowableEmitter<EventOutput>
    ) {
        try {
            //Running with "At least once" semantics
            consumer.commitSync()
        } catch (e: CommitFailedException) {
            log.error("Error commiting records to kafka", e)
            emitter.onError(e)
        }
    }

    private fun consumeRecord(
        record: ConsumerRecord<String, String>,
        emitter: FlowableEmitter<EventOutput>
    ) {
        try {
            val kafkaEvent: KafkaEvent = JsonSettings.mapper.readValue(record.value())
            emitter.onNext(kafkaEvent.toEventOutput())
        } catch (ex: Exception) {
            log.error("Error processing event", ex)
        }
    }
}