package mobi.waterdog.eventbus.engine.kafka

import com.fasterxml.jackson.module.kotlin.readValue
import io.reactivex.BackpressureStrategy
import io.reactivex.Flowable
import io.reactivex.FlowableEmitter
import mobi.waterdog.eventbus.model.EventOutput
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean

internal abstract class StreamStrategy {

    companion object {
        private val log = LoggerFactory.getLogger(this::class.java)
    }

    abstract fun stream(
        consumer: Consumer<String, String>,
        syncInterval: Long,
        isPollLoopStarted: AtomicBoolean,
        backpressureStrategy: BackpressureStrategy
    ): Flowable<EventOutput>

    protected fun createFlowable(strategy: BackpressureStrategy, onSubscribe: (FlowableEmitter<EventOutput>) -> Unit) =
        Flowable.create(onSubscribe, strategy)

    protected fun commitConsumedOffsets(
        consumer: Consumer<String, String>,
        emitter: FlowableEmitter<EventOutput>
    ) {
        try {
            consumer.commitSync()
        } catch (e: CommitFailedException) {
            log.error("Error commiting records to kafka", e)
            emitter.onError(e)
        }
    }

    protected fun consumeRecord(
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