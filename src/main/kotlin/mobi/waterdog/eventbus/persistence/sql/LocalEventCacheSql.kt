package mobi.waterdog.eventbus.persistence.sql

import mobi.waterdog.eventbus.model.Event
import mobi.waterdog.eventbus.model.EventInput
import mobi.waterdog.eventbus.persistence.LocalEventCache
import java.time.Instant
import java.util.UUID
import javax.sql.rowset.serial.SerialBlob

internal class LocalEventCacheSql(private val databaseConnection: DatabaseConnection) : LocalEventCache {
    override suspend fun markAsDelivered(eventId: Long) {
        databaseConnection.query {
            val event = EventDAO[eventId]
            event.delivered = true
        }
    }

    override suspend fun getAllUndelivered(): List<Event> {
        return databaseConnection.query {
            EventDAO.find { EventTable.delivered eq false }.map { it.toFullModel() }
        }
    }

    override suspend fun storeEvent(eventInput: EventInput): Event {
        return databaseConnection.query {
            EventDAO.new {
                topic = eventInput.topic
                delivered = false
                uuid = UUID.randomUUID().toString()
                sendTimestamp = Instant.now().toString()
                msgType = eventInput.msgType
                mimeType = eventInput.mimeType
                payload = SerialBlob(eventInput.payload)
            }.toFullModel()
        }
    }
}