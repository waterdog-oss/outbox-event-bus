package mobi.waterdog.eventbus.persistence

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mobi.waterdog.eventbus.EventProducer
import mobi.waterdog.eventbus.engine.EventEngine
import mobi.waterdog.eventbus.model.Event
import mobi.waterdog.eventbus.model.EventInput
import org.jetbrains.exposed.exceptions.EntityNotFoundException
import org.joda.time.Duration
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

internal class MaxRetriesExceededException(item: Event) :
    RuntimeException("Could not sync item #${item.id}. Max retries exceeded")

internal class PersistentEventWriter(
    private val localEventCache: LocalEventCache,
    private val eventEngine: EventEngine
) : EventProducer {

    companion object {
        private val log = LoggerFactory.getLogger(PersistentEventWriter::class.java)
        private val eventWriterLoopStarted = AtomicBoolean(false)

        fun shutdown() {
            eventWriterLoopStarted.set(false)
        }
    }

    init {
        if (!eventWriterLoopStarted.get()) {
            eventWriterLoopStarted.set(true)
            thread(name = "${PersistentEventWriter::class.simpleName}-SenderThread") {
                log.debug("SenderThread loop started")
                syncLoop()
            }

            thread(name = "${PersistentEventWriter::class.simpleName}-CacheSyncThread") {
                log.debug("CacheSyncThread loop started")
                cleanupLoop()
            }
        }
    }

    override fun sendAsync(eventInput: EventInput) {
        GlobalScope.launch {
            localEventCache.storeEvent(eventInput)
        }
    }

    override fun send(eventInput: EventInput): Boolean {
        return try {
            localEventCache.storeEvent(eventInput)
            true
        } catch (ex: Exception) {
            log.error("Event storage failed${ex.message}", ex)
            false
        }
    }

    private fun syncLoop() {
        while (eventWriterLoopStarted.get()) {
            val itemBatch = localEventCache.fetchEventsReadyToSend(100)
            if (itemBatch.isNotEmpty()) {
                itemBatch.forEach { item ->
                    try {
                        eventEngine.send(item)
                        val retries = 3 //TODO: Extract to conf
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
                        log.error("Error sending event", ex)
                        localEventCache.markAsErrored(item.id)
                    }
                }
            } else {
                runBlocking { delay(50) }
            }
        }
    }

    private fun cleanupLoop() {
        while (eventWriterLoopStarted.get()) {
            try {
                val itemsToCleanup =
                    localEventCache.fetchCleanableEvents(Duration.standardDays(7), 100) // TODO: Extract to conf
                if (itemsToCleanup.isNotEmpty()) {
                    itemsToCleanup.forEach { item ->
                        localEventCache.deleteEvent(item.id)
                    }
                } else {
                    runBlocking { delay(10000) }
                }
            } catch (ex: Exception) {
                log.error("Cleanup loop failed ${ex.message}")
                ex.printStackTrace()
            }
        }
    }
}
