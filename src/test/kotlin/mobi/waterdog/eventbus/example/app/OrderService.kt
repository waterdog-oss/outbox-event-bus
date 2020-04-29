package mobi.waterdog.eventbus.example.app

import mobi.waterdog.eventbus.EventProducer
import mobi.waterdog.eventbus.engine.kafka.JsonSettings
import mobi.waterdog.eventbus.model.EventInput
import mobi.waterdog.eventbus.persistence.sql.DatabaseConnection
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.insertAndGetId
import org.jetbrains.exposed.sql.select
import org.koin.core.KoinComponent
import org.koin.core.inject

class OrderService(private val topic: String, private val eventProducer: EventProducer) : KoinComponent {

    private val dbc: DatabaseConnection by inject()

    init {
        dbc.query {
            SchemaUtils.create(OrderTable)
            SchemaUtils.create(LineItemTable)
        }
    }

    fun createOrder(customer: String, lineItems: List<LineItem>, isEvil: Boolean = false): Order {
        return if (isEvil) createOrderEvilImpl(customer, lineItems) else createOrderImpl(
            customer,
            lineItems
        )
    }

    private fun createOrderImpl(customer: String, lineItems: List<LineItem>): Order = dbc.query {
        require(lineItems.isNotEmpty()) { "Line items must not be empty" }
        val newOrderId = OrderTable.insertAndGetId {
            it[customerName] = customer
        }

        val lineItemRepository = LineItemRepository(dbc)
        lineItems.forEach { lineItem ->
            lineItemRepository.insert(newOrderId.value, lineItem)
        }

        val order = Order(newOrderId.value, customer, lineItems)
        val eventPayload = JsonSettings.mapper.writeValueAsString(order).toByteArray()
        eventProducer.send(EventInput(topic, "orderCreated", "application/json", eventPayload))

        order
    }

    private fun createOrderEvilImpl(customer: String, lineItems: List<LineItem>): Order = dbc.query {
        require(lineItems.isNotEmpty()) { "Line items must not be empty" }
        val newOrderId = OrderTable.insertAndGetId {
            it[customerName] = customer
        }

        // For testing purposes the event is being sent earlier, just because...
        val order = Order(newOrderId.value, customer, lineItems)
        val eventPayload = JsonSettings.mapper.writeValueAsString(order).toByteArray()
        eventProducer.send(EventInput(topic, "orderCreated", "application/json", eventPayload))

        val lineItemRepository = LineItemRepository(dbc)
        lineItems.forEach { lineItem ->
            lineItemRepository.insert(newOrderId.value, lineItem)
        }

        order
    }

    fun getOrderById(orderId: Long): Order? = dbc.query {
        val result = OrderTable.select { OrderTable.id eq orderId }.firstOrNull()
        if (result != null) {
            val lineItems = LineItemTable.select { LineItemTable.orderId eq orderId }
                .map { LineItem(it[LineItemTable.productName], it[LineItemTable.quantity]) }

            Order(orderId, result[OrderTable.customerName], lineItems)
        } else {
            null
        }
    }
}
