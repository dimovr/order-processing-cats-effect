package com.example.stream

import cats.implicits.catsSyntaxOptionId
import cats.syntax.all.none
import com.example.model.{OrderRow, TransactionRow}

object OrderFsm {

  def ifPositive(order: OrderRow): Option[OrderRow] =
    if (order.filled > 0) order.some else none

  // todo: check negative amounts
  def toTransaction(existing: OrderRow, updated: OrderRow): Option[TransactionRow] =
    if (existing.filled != existing.total) TransactionRow(existing, updated).some else none
}
