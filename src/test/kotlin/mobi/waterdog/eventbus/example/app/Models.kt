package mobi.waterdog.eventbus.example.app

data class Order(val id: Long, val customer: String, val lineItems: List<LineItem>)
data class LineItem(val productName: String, val quantity: Int)