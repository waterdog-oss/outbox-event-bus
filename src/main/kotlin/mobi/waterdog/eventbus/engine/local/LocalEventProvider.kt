package mobi.waterdog.eventbus.engine.local

import mobi.waterdog.eventbus.EventBusProvider
import mobi.waterdog.eventbus.EventConsumer
import mobi.waterdog.eventbus.EventProducer
import mobi.waterdog.eventbus.engine.EventProvider
import mobi.waterdog.eventbus.persistence.EventRelay
import mobi.waterdog.eventbus.persistence.LocalEventStore
import java.time.Duration
import java.util.Properties

internal class LocalEventProvider : EventProvider {

    val engine: LocalEventEngine = LocalEventEngine()

    override fun getProducer(props: Properties, localEventStore: LocalEventStore): EventProducer {
        val cleanUpAfter: Duration =
            props.getProperty(EventBusProvider.CLEANUP_INTERVAL_SECONDS_PROP)?.toLong()?.let { Duration.ofSeconds(it) }
                ?: Duration.ofDays(7)
        return EventRelay(localEventStore, engine, cleanUpAfter)
    }

    override fun getConsumer(props: Properties): EventConsumer = engine
    override fun shutdown() {
        EventRelay.shutdown()
    }
}