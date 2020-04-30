package mobi.waterdog.eventbus.containers

import org.testcontainers.containers.PostgreSQLContainer

class KPostgreSQLContainer : PostgreSQLContainer<KPostgreSQLContainer>()

object PostgreSQLTestContainer {
    val instance by lazy { initPgSQL() }

    private fun initPgSQL(): KPostgreSQLContainer {
        val instance = KPostgreSQLContainer()
            .withDatabaseName("event_bus_test")
            .withUsername("test")
            .withPassword("test")
            .withCommand("postgres -c max_connections=1000")

        instance.start()
        return instance
    }
}