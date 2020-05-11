package mobi.waterdog.eventbus.engine

import io.micrometer.core.instrument.MeterRegistry
import mobi.waterdog.eventbus.EventConsumer
import mobi.waterdog.eventbus.EventProducer
import mobi.waterdog.eventbus.persistence.LocalEventStore
import java.util.Properties

internal interface EventProvider {
    fun getProducer(
        props: Properties = Properties(),
        localEventStore: LocalEventStore,
        meterRegistry: MeterRegistry
    ): EventProducer

    fun getConsumer(props: Properties = Properties(), meterRegistry: MeterRegistry): EventConsumer
    fun shutdown()
}