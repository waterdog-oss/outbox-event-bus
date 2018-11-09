package mobi.waterdog.eventbus

import java.util.Properties

enum class EventBackend {
    Local,
    Kafka
}

interface EventBusProvider {
    fun setup(type: EventBackend = EventBackend.Local, props: Properties = Properties())
    fun getProducer(): EventProducer
    fun getConsumer(): EventConsumer
}