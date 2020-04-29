package mobi.waterdog.eventbus.persistence.sql

import mobi.waterdog.eventbus.model.Event
import mobi.waterdog.eventbus.model.EventInput
import mobi.waterdog.eventbus.persistence.LocalEventCache
import org.jetbrains.exposed.sql.and
import org.joda.time.DateTime
import org.joda.time.Instant
import org.joda.time.ReadableDuration
import java.util.UUID
import javax.sql.rowset.serial.SerialBlob

internal class LocalEventCacheSql(private val databaseConnection: DatabaseConnection) : LocalEventCache {

    override fun markAsDelivered(eventId: Long) {
        databaseConnection.query {
            val event = EventDAO[eventId]
            event.delivered = true
            event.sendTimestamp = Instant.now().toDateTime()
        }
    }

    override fun markAsErrored(eventId: Long) {
        databaseConnection.query {
            val event = EventDAO[eventId]
            event.errored = false
        }
    }

    override fun getEvent(eventId: Long): Event? {
        return databaseConnection.query {
            EventDAO.find { EventTable.id eq eventId }.firstOrNull()?.toFullModel()
        }
    }

    override fun fetchEventsReadyToSend(limit: Int): List<Event> {
        return databaseConnection.query {
            EventDAO.find { (EventTable.delivered eq false) and (EventTable.errored eq false) }
                .limit(limit)
                .map { it.toFullModel() }
        }
    }

    override fun fetchCleanableEvents(duration: ReadableDuration, limit: Int): List<Event> {
        return databaseConnection.query {
            val cutoffTime = DateTime().minus(duration)
            EventDAO.find { (EventTable.delivered eq true) and (EventTable.storedTimestamp less cutoffTime) }
                .limit(limit)
                .map { it.toFullModel() }
        }
    }

    override fun deleteEvent(eventId: Long) {
        databaseConnection.query {
            EventDAO.findById(eventId)?.delete()
        }
    }

    override fun storeEvent(eventInput: EventInput): Event {
        return databaseConnection.query {
            EventDAO.new {
                topic = eventInput.topic
                delivered = false
                uuid = UUID.randomUUID().toString()
                storedTimestamp = Instant.now().toDateTime()
                msgType = eventInput.msgType
                mimeType = eventInput.mimeType
                payload = SerialBlob(eventInput.payload)
            }.toFullModel()
        }
    }
}