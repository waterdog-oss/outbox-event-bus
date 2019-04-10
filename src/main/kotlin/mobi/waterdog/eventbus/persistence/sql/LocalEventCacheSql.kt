package mobi.waterdog.eventbus.persistence.sql

import mobi.waterdog.eventbus.model.Event
import mobi.waterdog.eventbus.model.EventInput
import mobi.waterdog.eventbus.persistence.LocalEventCache
import org.joda.time.Instant
import java.util.UUID
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import javax.sql.rowset.serial.SerialBlob

internal class LocalEventCacheSql(private val databaseConnection: DatabaseConnection) : LocalEventCache {

    private val pendingEvents: BlockingQueue<Event> = LinkedBlockingQueue()

    override suspend fun markAsDelivered(eventId: Long) {

        databaseConnection.query {
            val event = EventDAO[eventId]
            event.delivered = true
            event.sendTimestamp = Instant.now().toDateTime()
        }
    }

    override suspend fun getEvent(eventId: Long): Event? {
        return databaseConnection.query {
            EventDAO.find { EventTable.id eq eventId }.firstOrNull()?.toFullModel()
        }
    }

    override fun getPendingEventQueue(): BlockingQueue<Event> {
        return pendingEvents
    }

    override suspend fun storeEvent(eventInput: EventInput): Event {
        return databaseConnection.query {
            val evt = EventDAO.new {
                topic = eventInput.topic
                delivered = false
                uuid = UUID.randomUUID().toString()
                storedTimestamp = Instant.now().toDateTime()
                msgType = eventInput.msgType
                mimeType = eventInput.mimeType
                payload = SerialBlob(eventInput.payload)
            }.toFullModel()

            this.pendingEvents.put(evt)
            evt
        }
    }
}