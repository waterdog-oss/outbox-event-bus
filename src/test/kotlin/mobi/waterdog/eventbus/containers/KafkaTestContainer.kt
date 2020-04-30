package mobi.waterdog.eventbus.containers

import org.testcontainers.containers.KafkaContainer

object KafkaTestContainer {
    val instance by lazy { initKafka() }

    private fun initKafka(): KafkaContainer {
        val instance = KafkaContainer()
        instance.start()
        return instance
    }
}
