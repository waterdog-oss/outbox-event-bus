package mobi.waterdog.eventbus.persistence

import kotlinx.coroutines.experimental.GlobalScope
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import mobi.waterdog.eventbus.EventProducer
import mobi.waterdog.eventbus.engine.EventEngine
import mobi.waterdog.eventbus.model.EventInput
import org.slf4j.LoggerFactory

internal class PersistentEventWriter(
    private val localEventCache: LocalEventCache,
    private val eventEngine: EventEngine,
    private val loopDelay: Long = 1000
) : EventProducer {

    private val log = LoggerFactory.getLogger(PersistentEventWriter::class.java)

    init {
        GlobalScope.launch {
            syncLoop()
        }
    }

    override fun send(eventInput: EventInput) {
        GlobalScope.launch {
            localEventCache.storeEvent(eventInput)
        }
    }

    override suspend fun sendAndWaitForAck(eventInput: EventInput): Boolean {
        return try {
            localEventCache.storeEvent(eventInput)
            true
        } catch (ex: Exception) {
            log.error("Event storage failed${ex.message}")
            ex.printStackTrace()
            false
        }
    }

    private suspend fun syncLoop() {
        while (true) {
            localEventCache.getAllUndelivered().forEach {
                try {
                    log.debug("Sending event to event ${it.topic}/${it.uuid} to backend")
                    eventEngine.send(it)
                    localEventCache.markAsDelivered(it.id)
                } catch (ex: Exception) {
                    log.error("Event sync failed${ex.message}")
                    ex.printStackTrace()
                }
            }
            delay(loopDelay)
        }
    }
}