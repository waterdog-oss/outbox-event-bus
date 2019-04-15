package mobi.waterdog.eventbus.example.producer

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mobi.waterdog.eventbus.EventBackend
import mobi.waterdog.eventbus.EventBusFactory
import mobi.waterdog.eventbus.EventProducer
import mobi.waterdog.eventbus.eventBusKoinModule
import mobi.waterdog.eventbus.model.EventInput
import org.koin.dsl.module.module
import org.koin.standalone.KoinComponent
import org.koin.standalone.StandAloneContext
import org.koin.standalone.inject
import java.time.Instant
import java.util.Properties
import javax.sql.DataSource

class ExampleProducer(kafkaServer: String) : KoinComponent {

    private val producer: EventProducer

    init {
        val props = Properties()
        //General cluster settings and config
        props["bootstrap.servers"] = kafkaServer
        //Kafka serialization config
        props["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        props["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"

        val ebp: EventBusFactory by inject()
        ebp.setup(EventBackend.Kafka)
        producer = ebp.getProducer(props)
    }

    fun startProducerLoop() {
        while (true) {
            producer.send(EventInput("test-0.10", "OK", "text/plain", "sent at: ${Instant.now()}".toByteArray()))
            runBlocking { delay(1000) }
        }
    }
}

fun main() {
    StandAloneContext.startKoin(
        listOf(
            module {
                single<DataSource> {
                    HikariDataSource(HikariConfig().apply {
                        driverClassName = "org.h2.Driver"
                        jdbcUrl = "jdbc:h2:mem:test"
                        maximumPoolSize = 5
                        isAutoCommit = false
                        transactionIsolation = "TRANSACTION_REPEATABLE_READ"
                        validate()
                    })
                }
            },
            eventBusKoinModule()
        )
    )

    val server = "kafka-service:9092"
    println("Starting producer: $server")
    val producer = ExampleProducer(server)
    producer.startProducerLoop()
    println("DONE")
}