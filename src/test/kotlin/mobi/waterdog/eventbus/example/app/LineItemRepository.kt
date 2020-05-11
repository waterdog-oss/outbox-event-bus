package mobi.waterdog.eventbus.example.app

import mobi.waterdog.eventbus.sql.DatabaseConnection
import org.jetbrains.exposed.dao.EntityID
import org.jetbrains.exposed.sql.insert

internal class LineItemRepository(private val dbc: DatabaseConnection) {

    fun insert(orderId: Long, lineItem: LineItem) = dbc.query {
        require(lineItem.quantity > 0) { "Quantity must be grater than zero" }

        LineItemTable.insert {
            it[productName] = lineItem.productName
            it[quantity] = lineItem.quantity
            it[LineItemTable.orderId] = EntityID(orderId, OrderTable)
        }
    }
}