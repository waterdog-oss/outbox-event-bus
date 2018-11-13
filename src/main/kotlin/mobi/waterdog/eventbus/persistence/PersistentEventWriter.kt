package mobi.waterdog.eventbus.persistence

import kotlinx.coroutines.experimental.GlobalScope
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import mobi.waterdog.eventbus.EventProducer
import mobi.waterdog.eventbus.engine.EventEngine
import mobi.waterdog.eventbus.model.EventInput
import org.slf4j.LoggerFactory
import kotlin.concurrent.thread

internal class PersistentEventWriter(
    private val localEventCache: LocalEventCache,
    private val eventEngine: EventEngine
) : EventProducer {

    private val log = LoggerFactory.getLogger(PersistentEventWriter::class.java)

    init {
        thread {
            syncLoop()
        }
    }

    override fun sendAsync(eventInput: EventInput) {
        GlobalScope.launch {
            localEventCache.storeEvent(eventInput)
        }
    }

    override fun send(eventInput: EventInput): Boolean {
        return runBlocking { sendAndWaitForAck(eventInput) }
    }

    private suspend fun sendAndWaitForAck(eventInput: EventInput): Boolean {
        return try {
            localEventCache.storeEvent(eventInput)
            true
        } catch (ex: Exception) {
            log.error("Event storage failed${ex.message}")
            ex.printStackTrace()
            false
        }
    }

    private fun syncLoop() {
        val queue = localEventCache.getPendingEventQueue()
        while (true) {
            try {
                val item = queue.take()
                log.trace("Sending event to event ${item.topic}/${item.uuid} to backend")
                eventEngine.send(item)
                localEventCache.markAsDelivered(item.id)
            } catch (ex: Exception) {
                log.error("Event sync failed${ex.message}")
                ex.printStackTrace()
            }
        }
    }
}
