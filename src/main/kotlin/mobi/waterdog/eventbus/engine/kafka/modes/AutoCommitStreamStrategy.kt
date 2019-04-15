package mobi.waterdog.eventbus.engine.kafka.modes

import io.reactivex.BackpressureStrategy
import io.reactivex.Flowable
import mobi.waterdog.eventbus.engine.kafka.StreamStrategy
import mobi.waterdog.eventbus.model.EventOutput
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.errors.WakeupException
import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

internal class AutoCommitStreamStrategy(props: Properties) : StreamStrategy() {

    init {
        require(props.getProperty("enable.auto.commit")?.toBoolean() == true) { "To use this stream mode, 'enable.auto.commit' must be true" }
        requireNotNull(props.getProperty("auto.commit.interval.ms")) { "To use this stream mode, 'auto.commit.interval.ms' must be set to a numeric value" }
    }

    override fun stream(
        consumer: Consumer<String, String>,
        syncInterval: Long,
        isPollLoopStarted: AtomicBoolean,
        backpressureStrategy: BackpressureStrategy
    ): Flowable<EventOutput> {
        return createFlowable(backpressureStrategy) { emitter ->
            try {
                while (isPollLoopStarted.get()) {
                    val records = consumer.poll(Duration.ofMillis(syncInterval))
                    for (record in records) {
                        consumeRecord(record, emitter)
                    }
                }
            } catch (wex: WakeupException) {
                //ignore
            } catch (ex: Exception) {
                emitter.onError(ex)
            } finally {
                consumer.close()
            }
        }
    }
}