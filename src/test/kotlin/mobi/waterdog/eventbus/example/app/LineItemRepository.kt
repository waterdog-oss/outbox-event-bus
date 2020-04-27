package mobi.waterdog.eventbus.example.app

import org.jetbrains.exposed.dao.EntityID
import org.jetbrains.exposed.sql.insert

class LineItemRepository() {

    fun insert(orderId: Long, lineItem: LineItem) {
        require(lineItem.quantity > 0) { "Quantity must be grater than zero" }

        LineItemTable.insert {
            it[productName] = lineItem.productName
            it[quantity] = lineItem.quantity
            it[LineItemTable.orderId] = EntityID(orderId, OrderTable)
        }
    }
}