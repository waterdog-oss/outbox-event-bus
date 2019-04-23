package mobi.waterdog.eventbus.persistence

import mobi.waterdog.eventbus.model.Event
import mobi.waterdog.eventbus.model.EventInput
import java.util.concurrent.BlockingQueue

internal interface LocalEventCache {
    fun storeEvent(eventInput: EventInput): Event
    fun getEvent(eventId: Long): Event?
    fun markAsDelivered(eventId: Long)
    fun getPendingEventQueue(): BlockingQueue<Event>
}