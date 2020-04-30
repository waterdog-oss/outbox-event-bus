package mobi.waterdog.eventbus

import java.util.Properties

enum class EventBackend {
    Local,
    Kafka
}

interface EventBusFactory {
    fun setup(type: EventBackend = EventBackend.Local)
    fun shutdown()
    fun getProducer(props: Properties = Properties()): EventProducer
    fun getConsumer(props: Properties = Properties()): EventConsumer
}