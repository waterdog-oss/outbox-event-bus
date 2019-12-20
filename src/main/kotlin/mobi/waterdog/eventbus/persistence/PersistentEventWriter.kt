package mobi.waterdog.eventbus.persistence

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mobi.waterdog.eventbus.EventProducer
import mobi.waterdog.eventbus.engine.EventEngine
import mobi.waterdog.eventbus.model.Event
import mobi.waterdog.eventbus.model.EventInput
import org.jetbrains.exposed.dao.exceptions.EntityNotFoundException
import org.slf4j.LoggerFactory
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.thread

internal class MaxRetriesExceededException(item: Event) :
    RuntimeException("Could not sync item #${item.id}. Max retries exceeded")

internal class PersistentEventWriter(
    private val localEventCache: LocalEventCache,
    private val eventEngine: EventEngine
) : EventProducer {

    companion object {
        private val log = LoggerFactory.getLogger(PersistentEventWriter::class.java)
    }

    private val sentItemsToSync = LinkedBlockingQueue<Event>()

    init {
        thread(name = "${PersistentEventWriter::class.simpleName}-SenderThread") {
            log.debug("SenderThread loop started")
            syncLoop()
        }
        thread(name = "${PersistentEventWriter::class.simpleName}-CacheSyncThread") {
            log.debug("CacheSyncThread loop started")
            cacheSyncLoop()
        }
    }

    override fun sendAsync(eventInput: EventInput) {
        GlobalScope.launch {
            localEventCache.storeEvent(eventInput)
        }
    }

    override fun send(eventInput: EventInput): Boolean {
        return sendAndWaitForAck(eventInput)
    }

    private fun sendAndWaitForAck(eventInput: EventInput): Boolean {
        return try {
            localEventCache.storeEvent(eventInput)
            true
        } catch (ex: Exception) {
            log.error("Event storage failed${ex.message}", ex)
            false
        }
    }

    private fun syncLoop() {
        val queue = localEventCache.getPendingEventQueue()
        while (true) {
            val item = queue.take()
            try {
                log.trace("Sending event to event ${item.topic}/${item.uuid} to backend")
                eventEngine.send(item)
                sentItemsToSync.put(item)
            } catch (ex: Exception) {
                log.error("Event sync ${item.id} failed${ex.message}", ex)
            }
        }
    }

    private fun cacheSyncLoop() {
        while (true) {
            val item = sentItemsToSync.take()
            val retries = 3 //TODO: Extract to conf
            try {
                var synced = false
                for (i in retries downTo 0) {
                    try {
                        localEventCache.markAsDelivered(item.id)
                        synced = true
                        break
                    } catch (ex: EntityNotFoundException) {
                        log.debug("Item not found in cache, wait...")
                        runBlocking { delay(50) } //TODO: Extract to conf
                    }
                }
                if (!synced) {
                    throw MaxRetriesExceededException(item)
                }
            } catch (ex: Exception) {
                log.error("CacheSyncThread failed ${ex.message}")
                ex.printStackTrace()
            }
        }
    }
}
