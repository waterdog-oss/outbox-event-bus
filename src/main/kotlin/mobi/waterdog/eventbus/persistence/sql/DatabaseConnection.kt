package mobi.waterdog.eventbus.persistence.sql

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.withContext
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.transaction
import javax.sql.DataSource

internal class DatabaseConnection(private val dataSource: DataSource, private val dispatcher: CoroutineDispatcher) {
    private val database: Database by lazy {
        Database.connect(dataSource)
    }

    suspend fun <T> query(block: () -> T): T = withContext(dispatcher) {
        transaction(database) {
            block()
        }
    }
}