package mobi.waterdog.eventbus

import mobi.waterdog.eventbus.model.EventInput

interface EventProducer {
    fun sendAsync(eventInput: EventInput)
    fun send(eventInput: EventInput): Boolean
}