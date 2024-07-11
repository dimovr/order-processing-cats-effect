package com.example.stream

import cats.effect.Deferred
import cats.effect.std.MapRef
import com.example.model.OrderId
import cats.data.EitherT
import cats.effect.implicits.monadCancelOps_
import cats.effect.{Ref, Resource}
import cats.effect.kernel.Async
import cats.effect.std.Queue
import fs2.{Pipe, Stream}
import org.typelevel.log4cats.Logger
import cats.syntax.all._
import com.example.model.{OrderRow, TransactionRow}
import com.example.persistence.PreparedQueries
import org.typelevel.log4cats.syntax.LoggerInterpolator
import skunk._

import scala.concurrent.duration.FiniteDuration

// All SQL queries inside the Queries object are correct and should not be changed
final class TransactionStream[F[_]](
  operationTimer: FiniteDuration,
  orders: Queue[F, OrderRow],
  session: Resource[F, Session[F]],
  transactionCounter: Ref[F, Int], // updated if long IO succeeds
  stateManager: StateManager[F],    // utility for state management
  mapRef: MapRef[F, OrderId, Option[Deferred[F, Unit]]],
  maxConcurrent: Int = 10
)(implicit F: Async[F], logger: Logger[F]) {

  private val processConcurrently: Pipe[F, OrderRow, Unit] =
    _.parEvalMapUnordered(maxConcurrent)(withRef)

  val stream: Stream[F, Unit] =
      Stream.fromQueueUnterminated(orders)
        .through(processConcurrently)

  private def withRef(order: OrderRow): F[Unit] =
    for {
      maybeLock <- mapRef(order.orderId).get
      _ <- maybeLock.traverse_(_.get)
      newLock <- Deferred[F, Unit]
      _ <- mapRef.setKeyValue(order.orderId, newLock)
      _ <- processUpdateWithCancellation(order)
      _ <- newLock.complete(())
      _ <- mapRef.unsetKey(order.orderId)
    } yield ()

  // ensures the processing completes even if the fiber is canceled
  private def processUpdateWithCancellation(order: OrderRow): F[Unit] =
    processUpdate(order).onCancel(processUpdate(order))

  // Application should shut down on error,
  // If performLongRunningOperation fails, we don't want to insert/update the records
  // Transactions always have positive amount
  // Order is executed if total == filled todo: what does order execution represent exactly? updated + insertTxn
  private def processUpdate(updatedOrder: OrderRow): F[Unit] = OrderFsm.ifPositive(updatedOrder) match {
    case Some(updatedOrder) =>
      PreparedQueries(session).use { queries =>
        for {
          // Get current known order state
          state <- stateManager.getOrderState(updatedOrder.orderId, queries)
          _ <- OrderFsm.toTransaction(state, updatedOrder) match {
            case Some(transaction) => tryUpdate(updatedOrder, transaction)(queries)
            case None => ().pure[F]
          }
        } yield ()
      }
    case None => ().pure[F]
  }

  private def tryUpdate(order: OrderRow, txn: TransactionRow)(queries: PreparedQueries[F]): F[Unit] =
    for {
      sp <- queries.xa.savepoint
      _ <- queries.updateOrder.execute(order.filled *: order.orderId *: EmptyTuple)
      _ <- queries.insertTransaction.execute(txn)
      _ <- performLongRunningOperation(txn).value.void.handleErrorWith(
            logger.error(_)(s"Got error when performing long running F!") *>
              queries.xa.rollback(sp).void
          )
    } yield ()


  // represents some long running effect that can fail
  private def performLongRunningOperation(txn: TransactionRow): EitherT[F, Throwable, Unit] = {
    EitherT.liftF[F, Throwable, Unit](
      F.sleep(operationTimer) *>
        stateManager.getSwitch.flatMap {
          case false =>
            transactionCounter.updateAndGet(_ + 1).flatMap(count =>
              info"Updated counter to $count by transaction with amount ${txn.amount} for order ${txn.orderId}!"
            )
          case true => F.raiseError(new Exception("Long running F failed!"))
        }
    )
  }

  // helper methods for testing
  def publish(update: OrderRow): F[Unit]                                          = orders.offer(update)
  def getCounter: F[Int]                                                          = transactionCounter.get
  def setSwitch(value: Boolean): F[Unit]                                          = stateManager.setSwitch(value)
  def addNewOrder(order: OrderRow, insert: PreparedCommand[F, OrderRow]): F[Unit] = stateManager.add(order, insert)
  // helper methods for testing
}

object TransactionStream {

  def apply[F[_]: Async: Logger](
    operationTimer: FiniteDuration,
    session: Resource[F, Session[F]]
  ): Resource[F, TransactionStream[F]] = {
    Resource.eval {
      for {
        counter      <- Ref.of(0)
        queue        <- Queue.unbounded[F, OrderRow]
        stateManager <- StateManager.apply
        mapRef <- MapRef.ofConcurrentHashMap[F, String, Deferred[F, Unit]]()
      } yield new TransactionStream[F](
        operationTimer,
        queue,
        session,
        counter,
        stateManager,
        mapRef
      )
    }
  }
}
