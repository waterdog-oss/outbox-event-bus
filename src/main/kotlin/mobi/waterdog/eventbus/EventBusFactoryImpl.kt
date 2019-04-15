package mobi.waterdog.eventbus

import mobi.waterdog.eventbus.engine.kafka.KafkaEngine
import mobi.waterdog.eventbus.engine.kafka.KafkaEventConsumer
import mobi.waterdog.eventbus.engine.local.LocalEventEngine
import mobi.waterdog.eventbus.persistence.LocalEventCache
import mobi.waterdog.eventbus.persistence.PersistentEventWriter
import java.util.Properties

internal interface EventBusProvider {
    fun getProducer(props: Properties = Properties()): EventProducer
    fun getConsumer(props: Properties = Properties()): EventConsumer
}

internal class EventBusFactoryImpl(private val localEventCache: LocalEventCache) : EventBusFactory {

    private var type: EventBackend? = null
    private var provider: EventBusProvider? = null

    override fun setup(type: EventBackend) {
        this.type = type
        this.provider = when (type) {
            EventBackend.Local -> InMemoryBus(localEventCache)
            EventBackend.Kafka -> KafkaBus(localEventCache)
        }
    }

    override fun getProducer(props: Properties): EventProducer {
        val provider = this.provider
        requireNotNull(provider) { "Provider not set. Please call 'setup'" }
        return provider.getProducer(props)
    }

    override fun getConsumer(props: Properties): EventConsumer {
        val provider = this.provider
        requireNotNull(provider) { "Provider not set. Please call 'setup'" }
        return provider.getConsumer(props)
    }
}

internal class InMemoryBus(private val localEventCache: LocalEventCache) : EventBusProvider {

    val engine: LocalEventEngine = LocalEventEngine()

    override fun getProducer(props: Properties): EventProducer = PersistentEventWriter(localEventCache, engine)
    override fun getConsumer(props: Properties): EventConsumer = engine
}

internal class KafkaBus(private val localEventCache: LocalEventCache) : EventBusProvider {
    override fun getProducer(props: Properties): EventProducer {
        return PersistentEventWriter(localEventCache, KafkaEngine(props))
    }

    override fun getConsumer(props: Properties): EventConsumer {
        return KafkaEventConsumer(props)
    }
}