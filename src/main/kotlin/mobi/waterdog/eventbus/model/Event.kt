package mobi.waterdog.eventbus.model

import java.time.Instant

internal data class Event(
    val id: Long,
    val delivered: Boolean,
    val topic: String,
    val uuid: String,
    val storeTimestamp: Instant,
    val sentTimestamp: Instant?,
    val msgType: String,
    val mimeType: String,
    val payload: ByteArray
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Event

        if (id != other.id) return false
        if (delivered != other.delivered) return false
        if (topic != other.topic) return false
        if (uuid != other.uuid) return false
        if (storeTimestamp != other.storeTimestamp) return false
        if (sentTimestamp != other.sentTimestamp) return false
        if (msgType != other.msgType) return false
        if (mimeType != other.mimeType) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + delivered.hashCode()
        result = 31 * result + topic.hashCode()
        result = 31 * result + uuid.hashCode()
        result = 31 * result + storeTimestamp.hashCode()

        if (sentTimestamp != null) {
            result = 31 * result + sentTimestamp.hashCode()
        }

        result = 31 * result + msgType.hashCode()
        result = 31 * result + mimeType.hashCode()
        return result
    }
}

data class EventInput(
    val topic: String,
    val msgType: String,
    val mimeType: String,
    val payload: ByteArray
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as EventInput

        if (topic != other.topic) return false
        if (msgType != other.msgType) return false
        if (mimeType != other.mimeType) return false

        return true
    }

    override fun hashCode(): Int {
        var result = topic.hashCode()
        result = 31 * result + msgType.hashCode()
        result = 31 * result + mimeType.hashCode()
        return result
    }
}

data class EventOutput(
    val uuid: String,
    val timestamp: Instant,
    val topic: String,
    val msgType: String,
    val mimeType: String,
    val payload: ByteArray
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as EventOutput

        if (uuid != other.uuid) return false
        if (timestamp != other.timestamp) return false
        if (topic != other.topic) return false
        if (msgType != other.msgType) return false
        if (mimeType != other.mimeType) return false

        return true
    }

    override fun hashCode(): Int {
        var result = uuid.hashCode()
        result = 31 * result + timestamp.hashCode()
        result = 31 * result + topic.hashCode()
        result = 31 * result + msgType.hashCode()
        result = 31 * result + mimeType.hashCode()
        return result
    }
}
