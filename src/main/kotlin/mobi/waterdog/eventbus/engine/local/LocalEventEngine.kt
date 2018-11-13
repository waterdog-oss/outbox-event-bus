package mobi.waterdog.eventbus.engine.local

import io.reactivex.BackpressureStrategy
import io.reactivex.Flowable
import io.reactivex.FlowableEmitter
import mobi.waterdog.eventbus.EventConsumer
import mobi.waterdog.eventbus.engine.EventEngine
import mobi.waterdog.eventbus.model.Event
import mobi.waterdog.eventbus.model.EventOutput
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

internal fun Event.toOutputEvent(): EventOutput = EventOutput(
    this.uuid,
    Instant.now(),
    this.topic,
    this.msgType,
    this.mimeType,
    this.payload
)

internal class LocalEventEngine : EventEngine, EventConsumer {

    private val subscribers: MutableMap<String, ConcurrentHashMap<String, (EventOutput) -> Unit>> = ConcurrentHashMap()

    override fun send(event: Event) {
        subscribers[event.topic]?.forEach { _, callback ->
            callback(event.toOutputEvent())
        }
    }

    override fun stream(topicName: String, consumerId: String): Flowable<EventOutput> {
        return createFlowable(BackpressureStrategy.ERROR) { emitter ->
            subscribe(topicName, consumerId) { event ->
                emitter.onNext(event)
            }
        }
    }

    override fun unsubscribe(topicName: String, consumerId: String) {
        subscribers[topicName]?.remove(consumerId)
    }

    override fun listSubscribers(topicName: String): List<String> {
        return when (subscribers[topicName]) {
            null -> listOf()
            else -> subscribers[topicName]!!.keys().toList()
        }
    }

    private fun subscribe(topicName: String, subscriberId: String, callback: (EventOutput) -> Unit) {
        if (subscribers[topicName] == null) {
            subscribers[topicName] = ConcurrentHashMap()
        }
        subscribers[topicName]!![subscriberId] = callback
    }

    private fun createFlowable(strategy: BackpressureStrategy, onSubscribe: (FlowableEmitter<EventOutput>) -> Unit) =
        Flowable.create(onSubscribe, strategy)
}