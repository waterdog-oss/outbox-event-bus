package mobi.waterdog.eventbus.engine

import mobi.waterdog.eventbus.model.Event

internal interface EventEngine {
    fun send(event: Event)
}