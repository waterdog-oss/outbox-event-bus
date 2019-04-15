package mobi.waterdog.eventbus

import mobi.waterdog.eventbus.persistence.sql.DatabaseConnection
import mobi.waterdog.eventbus.persistence.sql.EventTable
import mobi.waterdog.eventbus.persistence.sql.LocalEventCacheSql
import org.jetbrains.exposed.sql.SchemaUtils
import org.koin.dsl.module.Module
import org.koin.dsl.module.module

fun eventBusKoinModule(): Module {
    return module {
        val dbc = DatabaseConnection(get())
        val localEventCache = LocalEventCacheSql(dbc)

        dbc.query {
            SchemaUtils.create(EventTable)
        }

        single<EventBusFactory> { EventBusFactoryImpl(localEventCache) }
    }
}