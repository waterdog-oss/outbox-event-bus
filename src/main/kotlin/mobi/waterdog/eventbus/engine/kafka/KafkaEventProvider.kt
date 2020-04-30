package mobi.waterdog.eventbus.engine.kafka

import mobi.waterdog.eventbus.EventBusProvider
import mobi.waterdog.eventbus.EventConsumer
import mobi.waterdog.eventbus.EventProducer
import mobi.waterdog.eventbus.engine.EventProvider
import mobi.waterdog.eventbus.persistence.EventRelay
import mobi.waterdog.eventbus.persistence.LocalEventStore
import org.joda.time.Duration
import java.util.Properties

internal class KafkaEventProvider : EventProvider {

    override fun getProducer(props: Properties, localEventStore: LocalEventStore): EventProducer {
        val cleanUpAfter: Duration =
            props.getProperty(EventBusProvider.CLEANUP_INTERVAL_SECONDS_PROP)?.toLong()?.let { Duration.standardSeconds(it) }
                ?: Duration.standardDays(7)
        return EventRelay(localEventStore, KafkaEngine(props), cleanUpAfter)
    }

    override fun getConsumer(props: Properties): EventConsumer {
        return KafkaEventConsumer(props)
    }

    override fun shutdown() {
        EventRelay.shutdown()
    }
}