package mobi.waterdog.eventbus.persistence

import mobi.waterdog.eventbus.model.Event
import mobi.waterdog.eventbus.model.EventInput

internal interface LocalEventCache {
    suspend fun storeEvent(eventInput: EventInput): Event
    suspend fun markAsDelivered(eventId: Long)
    suspend fun getAllUndelivered(): List<Event>
}