package mobi.waterdog.eventbus.persistence

import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mobi.waterdog.eventbus.EventProducer
import mobi.waterdog.eventbus.engine.EventEngine
import mobi.waterdog.eventbus.model.EventInput
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

internal class EventRelay(
    private val localEventStore: LocalEventStore,
    private val eventEngine: EventEngine,
    private val cleanUpAfter: Duration,
    private val meterRegistry: MeterRegistry
) : EventProducer {

    companion object {
        private val log = LoggerFactory.getLogger(EventRelay::class.java)
        private val eventWriterLoopStarted = AtomicBoolean(false)
        private val currentGeneration = AtomicInteger(0)

        const val EVENTS_STORE_TIMER = "events.store.timer"
        const val EVENTS_STORE_ERROR = "events.store.error"
        const val EVENTS_SEND_TIMER = "events.send.timer"
        const val EVENTS_SEND_ERROR = "events.send.error"
        const val EVENTS_CLEANUP_TIMER = "events.cleanup.timer"
        const val EVENTS_CLEANUP_ERROR = "events.cleanup.error"

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
        val storeOpTimer = meterRegistry.timer(EVENTS_STORE_TIMER)
        val numStoreErrors = meterRegistry.counter(EVENTS_STORE_ERROR)
        return try {
            storeOpTimer.record {
                localEventStore.storeEvent(eventInput)
            }
            true
        } catch (ex: Exception) {
            log.error("Event storage failed${ex.message}", ex)
            numStoreErrors.increment()
            false
        }
    }

    private fun syncLoop(generation: Int) {
        val sendOpTimer = meterRegistry.timer(EVENTS_SEND_TIMER)
        val numSendErrors = meterRegistry.counter(EVENTS_SEND_ERROR)
        val retryAttempt = AtomicInteger(0)

        while (eventWriterLoopStarted.get() && currentGeneration.get() == generation) {
            val itemBatch = localEventStore.fetchEventsReadyToSend(100)
            if (itemBatch.isNotEmpty()) {
                for (item in itemBatch) {
                    try {
                        sendOpTimer.record {
                            eventEngine.send(item)
                            localEventStore.markAsDelivered(item.id)

                            // Reset retry attempt if needed
                            if (retryAttempt.get() > 0) {
                                retryAttempt.set(0)
                            }
                        }
                    } catch (ex: Exception) {
                        log.error("Error sending event", ex)
                        numSendErrors.increment()
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
        val cleanupOpTimer = meterRegistry.timer(EVENTS_CLEANUP_TIMER)
        val numCleanupErrors = meterRegistry.counter(EVENTS_CLEANUP_ERROR)

        while (eventWriterLoopStarted.get() && currentGeneration.get() == generation) {
            try {
                val itemsToCleanup =
                    localEventStore.fetchCleanableEvents(cleanUpAfter, 100)
                if (itemsToCleanup.isNotEmpty()) {
                    itemsToCleanup.forEach { item ->
                        cleanupOpTimer.record { localEventStore.deleteEvent(item.id) }
                    }
                } else {
                    runBlocking { delay(1000) }
                }
            } catch (ex: Exception) {
                log.error("Cleanup loop failed ${ex.message}", ex)
                numCleanupErrors.increment()
            }
        }
    }
}
