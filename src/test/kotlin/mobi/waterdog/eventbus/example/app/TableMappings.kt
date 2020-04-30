package mobi.waterdog.eventbus.example.app

import org.jetbrains.exposed.dao.LongIdTable
import org.jetbrains.exposed.sql.ReferenceOption

object OrderTable : LongIdTable("orders") {
    val customerName = varchar("customer", 255)
}

object LineItemTable : LongIdTable("line_items") {
    val productName = varchar("product_name", 255)
    val quantity = integer("quantity")
    val orderId = reference("order_id", OrderTable, ReferenceOption.CASCADE)
}
