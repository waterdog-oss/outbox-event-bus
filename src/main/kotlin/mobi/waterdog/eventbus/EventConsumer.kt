package mobi.waterdog.eventbus

import io.reactivex.Flowable
import mobi.waterdog.eventbus.model.EventOutput
import java.util.UUID

interface EventConsumer {
    fun stream(topicName: String, consumerId: String = UUID.randomUUID().toString()): Flowable<EventOutput>
    fun unsubscribe(topicName: String, consumerId: String)
    fun listSubscribers(topicName: String): List<String>
}