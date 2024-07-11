package com.example

import com.example.model.{OrderRow, TransactionRow}
import com.example.stream.OrderFsm
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util.UUID

class OrderFsmSpec extends AnyFlatSpec with Matchers {

  import OrderFsm._

  // Helper function to create OrderRow instances
  def createOrderRow(
                      orderId: String,
                      total: BigDecimal,
                      filled: BigDecimal,
                      createdAt: Instant = Instant.now(),
                      updatedAt: Instant = Instant.now()
                    ): OrderRow = OrderRow(orderId, "btc_eur", total, filled, createdAt, updatedAt)

  // Helper function to create TransactionRow instances
  def createTransactionRow(
                            orderId: String,
                            amount: BigDecimal,
                            createdAt: Instant = Instant.now()
                          ): TransactionRow = TransactionRow(UUID.randomUUID(), orderId, amount, createdAt)

  "OrderFsm.check" should "return NonPositiveOrder when incoming filled is non-positive" in {
    val existing = createOrderRow("1", 10, 5)

    val incomingZero = createOrderRow("1", 10, 0)
    OrderFsm.check(existing, incomingZero) shouldBe NonPositiveOrder

    val incomingNegative = createOrderRow("1", 10, -1)
    OrderFsm.check(existing, incomingNegative) shouldBe NonPositiveOrder
  }

  it should "return SmallerOrder when incoming filled is smaller than existing filled" in {
    val existing = createOrderRow("1", 10, 5)
    val incoming = createOrderRow("1", 10, 4)
    OrderFsm.check(existing, incoming) shouldBe SmallerOrder
  }

  it should "return FilledOrder when existing filled equals total" in {
    val existing = createOrderRow("1", 10, 10)
    val incoming = createOrderRow("1", 10, 5)
    OrderFsm.check(existing, incoming) shouldBe FilledOrder
  }

  it should "return ProcessOrder with updated order and new transaction when conditions are met" in {
    val existing = createOrderRow("1", 10, 5)
    val incoming = createOrderRow("1", 10, 7)

    OrderFsm.check(existing, incoming) match {
      case ProcessOrder(order, txn) =>
        order shouldBe incoming
        txn.orderId shouldBe existing.orderId
        txn.amount shouldBe (incoming.filled - existing.filled)
      case _ => fail("Expected ProcessOrder result")
    }
  }

  it should "handle edge cases correctly" in {
    // Incoming filled equals existing filled
    val existing1 = createOrderRow("1", 10, 5)
    val incoming1 = createOrderRow("1", 10, 5)
    OrderFsm.check(existing1, incoming1) match {
      case ProcessOrder(order, txn) =>
        order shouldBe incoming1
        txn.amount shouldBe 0
      case _ => fail("Expected ProcessOrder result")
    }

    // Incoming filled equals total (but existing doesn't)
    val existing2 = createOrderRow("2",  10, 8)
    val incoming2 = createOrderRow("2", 10, 10)
    OrderFsm.check(existing2, incoming2) match {
      case ProcessOrder(order, txn) =>
        order shouldBe incoming2
        txn.amount shouldBe 2
      case _ => fail("Expected ProcessOrder result")
    }
  }
}