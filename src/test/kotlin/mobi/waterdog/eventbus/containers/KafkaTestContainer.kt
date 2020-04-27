package mobi.waterdog.eventbus.containers

import org.testcontainers.containers.KafkaContainer

class KKafkaContainer : KafkaContainer()

object KafkaTestContainer {
    val instance by lazy { initKafka() }

    private fun initKafka(): KKafkaContainer {
        val instance = KKafkaContainer()
        instance.start()
        return instance
    }
}
