package mobi.waterdog.eventbus.engine

import mobi.waterdog.eventbus.EventConsumer
import mobi.waterdog.eventbus.EventProducer
import mobi.waterdog.eventbus.persistence.LocalEventStore
import java.util.Properties

internal interface EventProvider {
    fun getProducer(props: Properties = Properties(), localEventStore: LocalEventStore): EventProducer
    fun getConsumer(props: Properties = Properties()): EventConsumer
    fun shutdown()
}