package mobi.waterdog.eventbus.sql

import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.transaction
import javax.sql.DataSource

internal class DatabaseConnection(private val dataSource: DataSource) {
    private val database: Database by lazy {
        Database.connect(dataSource)
    }

    fun <T> query(block: () -> T): T = transaction(database) {
        block()
    }
}