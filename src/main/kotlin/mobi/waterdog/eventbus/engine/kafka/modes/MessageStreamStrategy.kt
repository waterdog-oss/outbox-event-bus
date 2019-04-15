package mobi.waterdog.eventbus.engine.kafka.modes

import io.reactivex.BackpressureStrategy
import io.reactivex.Flowable
import mobi.waterdog.eventbus.engine.kafka.StreamStrategy
import mobi.waterdog.eventbus.model.EventOutput
import org.apache.kafka.clients.consumer.Consumer
import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

internal class MessageStreamStrategy(props: Properties) : StreamStrategy() {

    init {
        require(props.getProperty("enable.auto.commit")?.toBoolean() == false) { "To use this stream mode, 'enable.auto.commit' must be false" }
    }

    override fun stream(
        consumer: Consumer<String, String>,
        syncInterval: Long,
        isPollLoopStarted: AtomicBoolean,
        backpressureStrategy: BackpressureStrategy
    ): Flowable<EventOutput> {
        return createFlowable(backpressureStrategy) { emitter ->
            while (isPollLoopStarted.get()) {
                val records = consumer.poll(Duration.ofMillis(syncInterval))
                for (record in records) {
                    consumeRecord(record, emitter)
                    commitConsumedOffsets(consumer, emitter)
                }
            }
            consumer.close()
        }
    }
}