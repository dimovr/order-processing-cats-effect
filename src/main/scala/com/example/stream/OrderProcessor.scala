package com.example.stream

import cats.effect.{Async, Ref}
import cats.implicits.{catsSyntaxApplicativeId, toFunctorOps}
import com.example.model.{OrderId, OrderRow}
import fs2.Stream

import scala.concurrent.duration.DurationInt

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

  private def concurrent[F[_]: Async](
      maxConcurrent: Int
  ): OrderProcessor[F] = new OrderProcessor[F] {

    override def process(stream: Stream[F, OrderRow], f: OrderRow => F[Unit]): Stream[F, Unit] =
      stream
        .through(StreamOps.groupBy[F, OrderRow, OrderId](_.orderId.pure[F]))
        .map { case (key, orderStream) =>
          println(s"$key -> $orderStream")
          orderStream
            .evalMap(f)
            .debounce(100.milliseconds)
        }
        .parJoin(maxConcurrent)

  }

  def apply[F[_]: Async](ps: ProcessingStrategy): OrderProcessor[F] = ps match {
    case ProcessingStrategy.Sequential      => sequential[F]
    case ProcessingStrategy.Concurrent(max) => concurrent[F](max)
  }

}
