package mobi.waterdog.eventbus.engine.kafka

import mobi.waterdog.eventbus.model.Event
import mobi.waterdog.eventbus.model.EventOutput
import java.time.Instant

internal class KafkaEvent(
    private val topic: String,
    private val uuid: String,
    private val timestamp: String,
    private val msgType: String,
    private val mimeType: String,
    private val payload: String
) {
    companion object {
        fun build(event: Event): KafkaEvent {
            return KafkaEvent(
                event.topic,
                event.uuid,
                Instant.now().toString(),
                event.msgType,
                event.mimeType,
                String(event.payload)
            )
        }
    }

    fun toEventOutput(): EventOutput = EventOutput(
        this.uuid,
        Instant.parse(this.timestamp),
        this.topic,
        this.msgType,
        this.mimeType,
        this.payload.toByteArray()
    )
}