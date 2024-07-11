package com.example.stream

import cats.syntax.all._
import cats.effect.std.MapRef
import cats.effect.{Async, Concurrent, Deferred}
import cats.implicits.toFunctorOps
import com.example.model.{OrderId, OrderRow}
import fs2.Stream

trait OrderProcessor[F[_]] {
  def process(
      s: Stream[F, OrderRow],
      f: OrderRow => F[Unit]
  ): Stream[F, Unit]
  // if needed, can be generalized into def process[T, R](
  //      s: Stream[F, T],
  //      f: T => F[R]
  //  ): Stream[F, R]... which is basically a Pipe
}

object OrderProcessor {

  sealed trait ProcessingStrategy
  object ProcessingStrategy {
    case object Sequential extends ProcessingStrategy
    case class Concurrent(maxConcurrent: Int) extends ProcessingStrategy
    object Concurrent {
      val Default: Concurrent = Concurrent(10)
    }
    val Default: ProcessingStrategy = Concurrent.Default
  }

  private def sequential[F[_]]: OrderProcessor[F] = new OrderProcessor[F] {
    override def process(stream: Stream[F, OrderRow], f: OrderRow => F[Unit]): Stream[F, Unit] =
      stream.evalMap(f)
  }

  private def concurrent[F[_]: Concurrent](
      maxConcurrent: Int,
      mapRef: MapRef[F, OrderId, Option[Deferred[F, Unit]]]
  ): OrderProcessor[F] = new OrderProcessor[F] {

    override def process(stream: Stream[F, OrderRow], f: OrderRow => F[Unit]): Stream[F, Unit] =
      stream.parEvalMapUnordered(maxConcurrent)(withRef(f))

    private def withRef(f: OrderRow => F[Unit])(order: OrderRow): F[Unit] =
      for {
        maybeLock <- mapRef(order.orderId).get
        _ <- maybeLock.traverse_(_.get)
        newLock <- Deferred[F, Unit]
        _ <- mapRef.setKeyValue(order.orderId, newLock)
        _ <- f(order)
        _ <- newLock.complete(())
        _ <- mapRef.unsetKey(order.orderId)
      } yield ()
  }

  private def concurrentOf[F[_]: Async](maxConcurrent: Int): F[OrderProcessor[F]] =
    MapRef.ofConcurrentHashMap[F, String, Deferred[F, Unit]]().map { ref =>
      concurrent[F](maxConcurrent, ref)
    }

  def of[F[_]: Async](ps: ProcessingStrategy): F[OrderProcessor[F]] = ps match {
    case ProcessingStrategy.Sequential      => sequential[F].pure[F]
    case ProcessingStrategy.Concurrent(max) => concurrentOf[F](max)
  }

}
