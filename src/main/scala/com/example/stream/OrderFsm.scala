package com.example.stream

import com.example.model.{OrderRow, TransactionRow}

object OrderFsm {

  sealed trait OrderProcessingResult
  case object FilledOrder extends OrderProcessingResult
  case object NonPositiveOrder extends OrderProcessingResult
  case object SmallerOrder extends OrderProcessingResult
  case class ProcessOrder(order: OrderRow, txn: TransactionRow) extends OrderProcessingResult

  def check(existing: OrderRow, incoming: OrderRow): OrderProcessingResult = {
    println(s"${existing.filled} -> ${incoming.filled}")
    if (existing.filled == existing.total)
      FilledOrder
    else if (incoming.filled <= 0)
      NonPositiveOrder
    else if (existing.filled > incoming.filled)
      SmallerOrder
    else {
      ProcessOrder(incoming, TransactionRow(existing, incoming))
    }
  }

}
