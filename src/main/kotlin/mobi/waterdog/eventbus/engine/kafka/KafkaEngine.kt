package mobi.waterdog.eventbus.engine.kafka

import mobi.waterdog.eventbus.engine.EventEngine
import mobi.waterdog.eventbus.model.Event
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import java.util.Properties

internal class KafkaEngine(producerConfigs: Properties) : EventEngine {

    private val log = LoggerFactory.getLogger(KafkaEngine::class.java)
    private val producer: Producer<String, String>

    init {
        producer = KafkaProducer(producerConfigs)
    }

    override fun send(event: Event) {
        log.debug("Forwarding event ${event.topic}/${event.uuid} to kafka")
        producer.send(ProducerRecord(event.topic, serializeEvent(event)))
    }

    private fun serializeEvent(event: Event): String {
        return JsonSettings.mapper.writeValueAsString(KafkaEvent.build(event))
    }
}