package mobi.waterdog.eventbus.engine.kafka

import com.fasterxml.jackson.module.kotlin.readValue
import io.reactivex.BackpressureStrategy
import io.reactivex.Flowable
import io.reactivex.FlowableEmitter
import mobi.waterdog.eventbus.model.EventOutput
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.errors.WakeupException
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
        } catch (e: WakeupException) {
            // we're shutting down, but finish the commit first and then
            // rethrow the exception so that the main loop can exit
            commitConsumedOffsets(consumer, emitter)
            emitter.onError(e)
        } catch (e: CommitFailedException) {
            // the commit failed with an unrecoverable error. if there is any
            // internal state which depended on the commit, you can clean it
            // up here. otherwise it's reasonable to ignore the error and go on
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