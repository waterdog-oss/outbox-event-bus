package mobi.waterdog.eventbus

import mobi.waterdog.eventbus.model.EventInput

interface EventProducer {
    fun send(eventInput: EventInput)
    suspend fun sendAndWaitForAck(eventInput: EventInput): Boolean
}