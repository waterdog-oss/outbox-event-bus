package mobi.waterdog.eventbus

import mobi.waterdog.eventbus.engine.EventEngine
import mobi.waterdog.eventbus.engine.kafka.KafkaEngine
import mobi.waterdog.eventbus.engine.kafka.KafkaEventConsumer
import mobi.waterdog.eventbus.engine.local.LocalEventEngine
import mobi.waterdog.eventbus.persistence.LocalEventCache
import mobi.waterdog.eventbus.persistence.PersistentEventWriter
import java.util.Properties

internal class EventBusFactory(private val localEventCache: LocalEventCache) : EventBusProvider {

    private var type: EventBackend? = null
    private var props: Properties? = null
    private var producerInstance: EventProducer? = null
    private var consumerInstance: EventConsumer? = null
    private var engine: EventEngine? = null

    override fun setup(type: EventBackend, props: Properties) {
        this.type = type
        this.props = props
        engine = when (type) {
            EventBackend.Local -> {
                LocalEventEngine()
            }
            EventBackend.Kafka -> {
                KafkaEngine(props)
            }
        }
    }

    override fun getProducer(): EventProducer {
        try {
            producerInstance = producerInstance ?: PersistentEventWriter(localEventCache, engine!!)
            return producerInstance!!
        } catch (npe: NullPointerException) {
            throw IllegalStateException("Please call setup")
        }
    }

    override fun getConsumer(): EventConsumer {
        consumerInstance = when (type) {
            EventBackend.Local -> {
                consumerInstance ?: engine!! as LocalEventEngine
            }
            EventBackend.Kafka -> {
                consumerInstance ?: KafkaEventConsumer(props!!)
            }
            null -> throw IllegalStateException("Please call setup")
        }
        return consumerInstance!!
    }
}