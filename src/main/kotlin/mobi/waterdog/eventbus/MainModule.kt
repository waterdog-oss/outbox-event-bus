package mobi.waterdog.eventbus

import mobi.waterdog.eventbus.persistence.sql.DatabaseConnection
import mobi.waterdog.eventbus.persistence.sql.EventTable
import mobi.waterdog.eventbus.persistence.sql.LocalEventCacheSql
import org.jetbrains.exposed.sql.SchemaUtils
import org.koin.core.module.Module
import org.koin.dsl.module

fun eventBusKoinModule(): Module {
    return module {
        single<EventBusFactory> {
            val dbc = DatabaseConnection(get())
            val localEventCache = LocalEventCacheSql(dbc)

            dbc.query {
                SchemaUtils.create(EventTable)
            }

            EventBusFactoryImpl(localEventCache)
        }
    }
}