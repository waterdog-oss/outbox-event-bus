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
    val sendTimestamp = varchar("send_timestamp", 24)
    val msgType = varchar("msgType", 255)
    val mimeType = varchar("mimeType", 255)
    val payload = blob("payload")
}

internal class EventDAO(id: EntityID<Long>) : LongEntity(id) {
    companion object : LongEntityClass<EventDAO>(EventTable)

    var topic by EventTable.topic
    var delivered by EventTable.delivered
    var uuid by EventTable.uuid
    var sendTimestamp by EventTable.sendTimestamp
    var msgType by EventTable.msgType
    var mimeType by EventTable.mimeType
    var payload by EventTable.payload

    fun toFullModel(): Event {
        return Event(
            id.value,
            delivered,
            topic,
            uuid,
            Instant.parse(sendTimestamp),
            msgType,
            mimeType,
            payload.binaryStream.readBytes()
        )
    }
}