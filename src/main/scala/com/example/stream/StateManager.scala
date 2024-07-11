package com.example.stream

import cats.effect.implicits.{genSpawnOps, genTemporalOps_}
import cats.effect.{Async, Deferred}
import com.example.model.{OrderId, OrderRow}
import com.example.persistence.PreparedQueries
import skunk.PreparedCommand
import cats.syntax.all._
import fs2.concurrent.SignallingRef

import scala.concurrent.duration.DurationInt

//Utility for managing placed order state
//This can be used by other components, for example a stream that performs order placement will use add method
final class StateManager[F[_]: Async](ioSwitch: SignallingRef[F, Boolean]) {

  def getOrderState(orderId: OrderId, queries: PreparedQueries[F]): F[OrderRow] = {
    def retryUntilFound(deferred: Deferred[F, OrderRow]): F[Unit] = {
      val attemptGet = queries.getOrder.unique(orderId).attempt

      def loop: F[Unit] = attemptGet.flatMap {
        case Right(order) => deferred.complete(order).void
        case Left(_) => loop
      }

      loop
    }

    for {
      deferred <- Deferred[F, OrderRow]
      _ <- retryUntilFound(deferred).start // unsafe?
      result <- deferred.get.timeoutTo(5.seconds, new Exception(s"No order found for id $orderId").raiseError) // todo: add custom errors
    } yield result
  }

  def add(row: OrderRow, insert: PreparedCommand[F, OrderRow]): F[Unit] = {
    insert.execute(row).void
  }

  def getSwitch: F[Boolean]              = ioSwitch.get
  def setSwitch(value: Boolean): F[Unit] = ioSwitch.set(value)
}

object StateManager {

  def apply[F[_]: Async]: F[StateManager[F]] = {
    SignallingRef.of(false).map(new StateManager[F](_))
  }
}
