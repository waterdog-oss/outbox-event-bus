package mobi.waterdog.eventbus.persistence

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mobi.waterdog.eventbus.EventProducer
import mobi.waterdog.eventbus.engine.EventEngine
import mobi.waterdog.eventbus.model.EventInput
import org.joda.time.Duration
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

internal class EventRelay(
    private val localEventCache: LocalEventCache,
    private val eventEngine: EventEngine,
    private val cleanUpAfter: Duration
) : EventProducer {

    companion object {
        private val log = LoggerFactory.getLogger(EventRelay::class.java)
        private val eventWriterLoopStarted = AtomicBoolean(false)
        private val currentGeneration = AtomicInteger(0)

        fun shutdown() {
            eventWriterLoopStarted.set(false)
        }
    }

    init {
        if (!eventWriterLoopStarted.get()) {
            eventWriterLoopStarted.set(true)
            val currentVersion = currentGeneration.incrementAndGet()
            thread(name = "${EventRelay::class.simpleName}-SenderThread") {
                log.debug("SenderThread loop started")
                syncLoop(currentVersion)
            }

            thread(name = "${EventRelay::class.simpleName}-CleanupThread") {
                log.debug("CleanupThread loop started")
                cleanupLoop(currentVersion)
            }
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

    private fun syncLoop(generation: Int) {
        val retryAttempt = AtomicInteger(0)
        while (eventWriterLoopStarted.get() && currentGeneration.get() == generation) {
            val itemBatch = localEventCache.fetchEventsReadyToSend(100)
            if (itemBatch.isNotEmpty()) {
                for (item in itemBatch) {
                    try {
                        eventEngine.send(item)
                        localEventCache.markAsDelivered(item.id)
                        retryAttempt.set(0)
                    } catch (ex: Exception) {
                        log.error("Error sending event", ex)
                        val attempt = retryAttempt.incrementAndGet()
                        runBlocking { delay((attempt * 50L).coerceAtMost(1000L)) }
                        break
                    }
                }
            } else {
                runBlocking { delay(50) }
            }
        }
    }

    private fun cleanupLoop(generation: Int) {
        while (eventWriterLoopStarted.get() && currentGeneration.get() == generation) {
            try {
                println("CLEANUP LIMIT: ${cleanUpAfter.toStandardSeconds()}")
                val itemsToCleanup =
                    localEventCache.fetchCleanableEvents(cleanUpAfter, 100)
                if (itemsToCleanup.isNotEmpty()) {
                    itemsToCleanup.forEach { item ->
                        localEventCache.deleteEvent(item.id)
                    }
                } else {
                    runBlocking { delay(1000) }
                }
            } catch (ex: Exception) {
                log.error("Cleanup loop failed ${ex.message}")
                ex.printStackTrace()
            }
        }
    }
}
