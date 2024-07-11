package com.example.model

import java.time.Instant

case class OrderRow(
  orderId: OrderId,
  market: String,
  total: BigDecimal,
  filled: BigDecimal, //state of completion of the order
  createdAt: Instant,
  updatedAt: Instant
)
