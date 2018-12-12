package mobi.waterdog.eventbus.persistence

import mobi.waterdog.eventbus.model.Event
import mobi.waterdog.eventbus.model.EventInput
import java.util.concurrent.BlockingQueue

internal interface LocalEventCache {
    suspend fun storeEvent(eventInput: EventInput): Event
    suspend fun getEvent(eventId: Long): Event?
    suspend fun markAsDelivered(eventId: Long)
    fun getPendingEventQueue(): BlockingQueue<Event>
}