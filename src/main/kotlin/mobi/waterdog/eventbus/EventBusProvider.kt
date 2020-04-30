package mobi.waterdog.eventbus

import mobi.waterdog.eventbus.engine.EventProvider
import mobi.waterdog.eventbus.engine.kafka.KafkaEventProvider
import mobi.waterdog.eventbus.engine.local.LocalEventProvider
import mobi.waterdog.eventbus.persistence.LocalEventStore
import java.util.Properties

class EventBusProvider(type: EventBackend) {

    companion object {
        const val CLEANUP_INTERVAL_SECONDS_PROP = "producer.event.cleanup.intervalInSeconds"
    }

    private val provider: EventProvider = when (type) {
        EventBackend.Local -> LocalEventProvider()
        EventBackend.Kafka -> KafkaEventProvider()
    }

    private var localEventStore: LocalEventStore? = null

    fun setupProducer(localEventStore: LocalEventStore) {
        this.localEventStore = localEventStore
    }

    fun shutdown() {
        this.provider.shutdown()
    }

    fun getProducer(props: Properties = Properties()): EventProducer {
        val store = localEventStore
        requireNotNull(store) {
            "Local event store has not been defined. Please run 'setupProvider'"
        }
        return provider.getProducer(props, store)
    }

    fun getConsumer(props: Properties = Properties()): EventConsumer {
        return provider.getConsumer(props)
    }
}