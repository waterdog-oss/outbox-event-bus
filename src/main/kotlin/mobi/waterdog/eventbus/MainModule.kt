package mobi.waterdog.eventbus

import kotlinx.coroutines.experimental.runBlocking
import mobi.waterdog.eventbus.persistence.sql.DatabaseConnection
import mobi.waterdog.eventbus.persistence.sql.EventTable
import mobi.waterdog.eventbus.persistence.sql.LocalEventCacheSql
import org.jetbrains.exposed.sql.SchemaUtils
import org.koin.dsl.module.Module
import org.koin.dsl.module.module

fun getModule(initTables: Boolean = true): Module {
    return module {
        val dbc = DatabaseConnection(get(), get())
        val localEventCache = LocalEventCacheSql(dbc)

        if (initTables) {
            runBlocking {
                dbc.query {
                    SchemaUtils.create(EventTable)
                }
            }
        }

        single<EventBusProvider> { EventBusFactory(localEventCache) }
    }
}