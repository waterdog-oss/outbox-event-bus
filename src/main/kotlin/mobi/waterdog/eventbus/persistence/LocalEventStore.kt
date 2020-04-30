package mobi.waterdog.eventbus.persistence

import mobi.waterdog.eventbus.model.Event
import mobi.waterdog.eventbus.model.EventInput
import java.time.Duration

interface LocalEventStore {
    fun storeEvent(eventInput: EventInput): Event
    fun getEvent(eventId: Long): Event?
    fun markAsDelivered(eventId: Long)
    fun fetchEventsReadyToSend(limit: Int): List<Event>
    fun fetchCleanableEvents(duration: Duration, limit: Int): List<Event>
    fun deleteEvent(eventId: Long)
}