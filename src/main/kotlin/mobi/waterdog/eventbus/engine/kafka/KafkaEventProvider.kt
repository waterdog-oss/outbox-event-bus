package mobi.waterdog.eventbus.engine.kafka

import io.micrometer.core.instrument.MeterRegistry
import mobi.waterdog.eventbus.EventBusProvider
import mobi.waterdog.eventbus.EventConsumer
import mobi.waterdog.eventbus.EventProducer
import mobi.waterdog.eventbus.engine.EventProvider
import mobi.waterdog.eventbus.persistence.EventRelay
import mobi.waterdog.eventbus.persistence.LocalEventStore
import java.time.Duration
import java.util.Properties

internal class KafkaEventProvider : EventProvider {

    override fun getProducer(
        props: Properties,
        localEventStore: LocalEventStore,
        meterRegistry: MeterRegistry
    ): EventProducer {
        val cleanUpAfter: Duration =
            props.getProperty(EventBusProvider.CLEANUP_INTERVAL_SECONDS_PROP)?.toLong()?.let { Duration.ofSeconds(it) }
                ?: Duration.ofDays(7)
        return EventRelay(localEventStore, KafkaEngine(props), cleanUpAfter, meterRegistry)
    }

    override fun getConsumer(props: Properties, meterRegistry: MeterRegistry): EventConsumer {
        return KafkaEventConsumer(props)
    }

    override fun shutdown() {
        EventRelay.shutdown()
    }
}