package mobi.waterdog.eventbus.persistence.sql

import mobi.waterdog.eventbus.model.Event
import org.jetbrains.exposed.dao.EntityID
import org.jetbrains.exposed.dao.LongEntity
import org.jetbrains.exposed.dao.LongEntityClass
import org.jetbrains.exposed.dao.LongIdTable

internal object EventTable : LongIdTable("recorded_events") {
    val topic = varchar("topic", 255)
    val delivered = bool("delivered")
    val errored = bool("errored").default(false)
    val uuid = varchar("uuid", 36).uniqueIndex()
    val storedTimestamp = datetime("stored_timestamp")
    val sendTimestamp = datetime("send_timestamp").nullable()
    val msgType = varchar("msg_type", 255)
    val mimeType = varchar("mime_type", 255)
    val payload = blob("payload")
}

internal class EventDAO(id: EntityID<Long>) : LongEntity(id) {
    companion object : LongEntityClass<EventDAO>(EventTable)

    var topic by EventTable.topic
    var delivered by EventTable.delivered
    var uuid by EventTable.uuid
    var storedTimestamp by EventTable.storedTimestamp
    var sendTimestamp by EventTable.sendTimestamp
    var msgType by EventTable.msgType
    var mimeType by EventTable.mimeType
    var payload by EventTable.payload
    var errored by EventTable.errored

    fun toFullModel(): Event {
        return Event(
            id.value,
            delivered,
            errored,
            topic,
            uuid,
            java.time.Instant.ofEpochMilli(storedTimestamp.toInstant().millis),
            sendTimestamp?.let { java.time.Instant.ofEpochMilli(it.toInstant().millis) },
            msgType,
            mimeType,
            payload.binaryStream.readBytes()
        )
    }
}