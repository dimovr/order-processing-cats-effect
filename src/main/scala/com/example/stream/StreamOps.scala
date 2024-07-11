package com.example.stream

import cats.effect.{Concurrent, Ref}
import cats.effect.std.Queue
import fs2.{Pipe, Stream}
import cats.implicits._

private[stream] object StreamOps {

  def groupBy[F[_]: Concurrent, A, K](
      selector: A => F[K]
  ): Pipe[F, A, (K, Stream[F, A])] = in =>
    Stream.eval(Ref.of[F, Map[K, Queue[F, Option[A]]]](Map.empty)).flatMap { queueMap =>
      val cleanup = queueMap.get.flatMap(_.values.toList.traverse_(_.offer(None)))

      (in ++ Stream.exec(cleanup))
        .evalMap { elem =>
          (selector(elem), queueMap.get).mapN { (key, queues) =>
            queues.get(key).fold {
              for {
                newQ <- Queue.unbounded[F, Option[A]]
                _    <- queueMap.modify(queues => (queues + (key -> newQ), queues))
                _ <- newQ.offer(elem.some)
              } yield (key -> Stream.fromQueueNoneTerminated(newQ, 100)).some
            }(_.offer(elem.some).as(None))
          }.flatten
        }
        .unNone
        .onFinalize(cleanup)
    }

}
