package com.example.model

import java.time.Instant
import java.util.UUID

case class TransactionRow(
  id: UUID,
  orderId: OrderId, // the same as OrderRow
  amount: BigDecimal,
  createdAt: Instant
)

object TransactionRow {

  def apply(state: OrderRow, updated: OrderRow): TransactionRow = {
//    val amount = Math.max(updated.filled.toDouble, state.filled.toDouble) -
//      Math.min(updated.filled.toDouble, state.filled.toDouble)
    val amount = updated.filled - state.filled
    println(amount)

    TransactionRow(
      id = UUID.randomUUID(), // generate some id for our transaction
      orderId = state.orderId,
      // should be validated in an FSM; theoretically each next update should have bigger value for filled
      amount = amount,
      createdAt = Instant.now()
    )
  }
}
