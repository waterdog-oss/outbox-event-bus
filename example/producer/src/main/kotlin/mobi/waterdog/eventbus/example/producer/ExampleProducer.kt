package mobi.waterdog.eventbus.example.producer

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mobi.waterdog.eventbus.model.EventInput
import mobi.waterdog.eventbus.persistence.LocalEventCache
import mobi.waterdog.eventbus.sql.DatabaseConnection
import mobi.waterdog.eventbuse.example.sql.EventTable
import mobi.waterdog.eventbus.example.sql.LocalEventStoreSql
import org.jetbrains.exposed.example.sql.SchemaUtils
import org.koin.core.KoinComponent
import org.koin.core.context.startKoin
import org.koin.core.inject
import org.koin.dsl.module
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

        val localEventCache: LocalEventCache by inject()
        val ebp = EventBusProvider()
        ebp.setup(EventBackend.Kafka)
        producer = ebp.getProducer(props, localEventCache)
    }

    fun startProducerLoop() {
        while (true) {
            producer.send(EventInput("test-0.10", "OK", "text/plain", "sent at: ${Instant.now()}".toByteArray()))
            runBlocking { delay(1000) }
        }
    }
}

fun main() {
    startKoin {
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
            module {
                single {
                    DatabaseConnection(get())
                }
            },
            module {
                single<LocalEventCache> {
                    val dbc: DatabaseConnection = get()
                    val localEventStoreSql = LocalEventStoreSql(dbc)
                    dbc.query {
                        SchemaUtils.create(EventTable)
                    }
                    localEventStoreSql
                }
            }
        )
    }

    val server = "kafka-service:9092"
    println("Starting producer: $server")
    val producer = ExampleProducer(server)
    producer.startProducerLoop()
    println("DONE")
}