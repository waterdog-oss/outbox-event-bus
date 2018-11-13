package mobi.waterdog.eventbus.persistence.sql

import mobi.waterdog.eventbus.model.Event
import org.jetbrains.exposed.dao.EntityID
import org.jetbrains.exposed.dao.LongEntity
import org.jetbrains.exposed.dao.LongEntityClass
import org.jetbrains.exposed.dao.LongIdTable
import java.time.Instant

internal object EventTable : LongIdTable("recorded_events") {
    val topic = varchar("topic", 255)
    val delivered = bool("delivered")
    val uuid = varchar("uuid", 36).uniqueIndex()
    val storedTimestamp = varchar("stored_timestamp", 24)
    val sendTimestamp = varchar("send_timestamp", 24).nullable()
    val msgType = varchar("msgType", 255)
    val mimeType = varchar("mimeType", 255)
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

    fun toFullModel(): Event {
        val sendTs = if (sendTimestamp != null) Instant.parse(sendTimestamp) else null
        return Event(
            id.value,
            delivered,
            topic,
            uuid,
            Instant.parse(storedTimestamp),
            sendTs,
            msgType,
            mimeType,
            payload.binaryStream.readBytes()
        )
    }
}