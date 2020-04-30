package mobi.waterdog.eventbus.containers

import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.ToxiproxyContainer

private var network: Network = Network.newNetwork()

object ToxiProxy {
    val instance by lazy { initToxiProxy() }

    private fun initToxiProxy(): ToxiproxyContainer {
        val instance = ToxiproxyContainer()
            .withNetwork(network)
        instance.start()
        return instance
    }
}

object ProxiedKafkaTestContainer {
    val instance by lazy { initKafka() }

    private fun initKafka(): KafkaContainer {
        val instance = KafkaContainer()
            .withNetwork(network)
            .withExposedPorts(9093, 2181)
        instance.start()
        return instance
    }
}