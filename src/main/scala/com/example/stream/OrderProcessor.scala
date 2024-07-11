package com.example.stream

import cats.effect.Async
import cats.implicits.catsSyntaxApplicativeId
import com.example.model.{OrderId, OrderRow}
import fs2.Pipe

import scala.concurrent.duration.DurationInt

trait OrderProcessor[F[_]] extends ((OrderRow => F[Unit]) => Pipe[F, OrderRow, Unit])

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

  def sequential[F[_]]: OrderProcessor[F] = (f: OrderRow => F[Unit]) =>
      _.evalMap(f)

  def concurrent[F[_]: Async](
      maxConcurrent: Int
  ): OrderProcessor[F] = (f: OrderRow => F[Unit]) =>
      _.through(StreamOps.groupBy[F, OrderRow, OrderId](_.orderId.pure[F]))
        .map { case (_, orderStream) =>
          orderStream
            .evalMap(f)
            .debounce(100.milliseconds)
        }.parJoin(maxConcurrent)


  // used only in test to demonstrate the difference with `concurrent`
  def testConcurrent[F[_]: Async](
      maxConcurrent: Int
  ): OrderProcessor[F] = (f: OrderRow => F[Unit]) =>
    _.parEvalMap(maxConcurrent)(f)

  def apply[F[_]: Async](ps: ProcessingStrategy): OrderProcessor[F] = ps match {
    case ProcessingStrategy.Sequential      => sequential[F]
    case ProcessingStrategy.Concurrent(max) => concurrent[F](max)
  }

}
