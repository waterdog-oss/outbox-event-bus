package mobi.waterdog.eventbus

import mobi.waterdog.eventbus.persistence.LocalEventStore
import mobi.waterdog.eventbus.sql.DatabaseConnection
import mobi.waterdog.eventbus.sql.EventTable
import mobi.waterdog.eventbus.sql.LocalEventStoreSql
import org.jetbrains.exposed.sql.SchemaUtils
import org.koin.core.module.Module
import org.koin.dsl.module

fun databaseConnectionModule(): Module {
    return module {
        single {
            DatabaseConnection(get())
        }
    }
}

fun localEventCacheModule(): Module {
    return module {
        single<LocalEventStore> {
            val dbc: DatabaseConnection = get()
            val localEventCache = LocalEventStoreSql(dbc)
            dbc.query {
                SchemaUtils.create(EventTable)
            }
            localEventCache
        }
    }
}

fun eventBusKoinModule(): Module {
    return module {
        single {
            EventBusProvider(EventBackend.Kafka)
        }
    }
}