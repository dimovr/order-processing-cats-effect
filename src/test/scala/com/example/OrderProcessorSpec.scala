package com.example

import cats.effect._
import cats.effect.testing.scalatest.AsyncIOSpec
import com.example.model.{OrderId, OrderRow}
import com.example.stream.OrderProcessor
import fs2.Stream
import org.scalatest.matchers.must.Matchers.convertToAnyMustWrapper
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class OrderProcessorSpec extends AsyncWordSpec with AsyncIOSpec with Matchers with OptionValues {

  implicit override def executionContext: ExecutionContext = ExecutionContext.global

  "OrderProcessor" should {

    "process orders sequentially" in {
      for {
        processedOrders <- Ref[IO].of(Vector.empty[OrderRow])
        processOrder = (order: OrderRow) =>
          IO.sleep(10.millis) *> processedOrders.update(_ :+ order)
        processor = OrderProcessor.sequential[IO]
        orders = List(
          OrderRow("1", "btc_eur", 50, 10, Instant.now, Instant.now),
          OrderRow("2", "btc_eur", 50, 20, Instant.now, Instant.now),
          OrderRow("1", "btc_eur", 50, 30, Instant.now, Instant.now),
          OrderRow("3", "btc_eur", 50, 40, Instant.now, Instant.now),
          OrderRow("2", "btc_eur", 50, 50, Instant.now, Instant.now)
        )
        _ <- Stream.emits(orders).through(processor(processOrder)).compile.drain
        result <- processedOrders.get
      } yield result mustBe orders
    }

    "should process concurrently for different order ids equal to the number of different ids" in {
      val processor = OrderProcessor.concurrent[IO](10)
      val now = Instant.now
      val orders = List(
        OrderRow("1", "btc_eur", 100, 10, now, now),
        OrderRow("2", "btc_eur", 100, 20, now, now),
        OrderRow("1", "btc_eur", 100, 30, now, now),
        OrderRow("2", "btc_eur", 100, 20, now, now),
        OrderRow("3", "btc_eur", 100, 50, now, now),
        OrderRow("1", "btc_eur", 100, 60, now, now),
        OrderRow("5", "btc_eur", 100, 70, now, now),
        OrderRow("4", "btc_eur", 100, 80, now, now),
        OrderRow("2", "btc_eur", 100, 90, now, now)
      )

      for {
        currentlyProcessing <- Ref.of[IO, Int](0)
        maxConcurrencyRef <- Ref.of[IO, Int](0)
        processedRef <- Ref[IO].of(Vector.empty[OrderRow])
        processOrder = (order: OrderRow) =>
          for {
            _ <- currentlyProcessing.update(_ + 1)
            current <- currentlyProcessing.get
            _ <- maxConcurrencyRef.update(math.max(current, _))
            _ <- IO.sleep(200.milliseconds) // Simulate some processing time
            _ <- currentlyProcessing.update(_ - 1)
            _ <- processedRef.update(_ :+ order)
          } yield ()
        _ <- Stream.emits(orders)
          .covary[IO]
          .metered(10.milliseconds)
          .through(processor(processOrder))
          .compile
          .drain
        _ <- IO.sleep(200.milliseconds)
        processed <- processedRef.get
        maxConcurrency <- maxConcurrencyRef.get
      } yield {
        processed.size shouldBe 9
        maxConcurrency shouldBe 5
      }
    }

    "should process concurrently to the configured limit regardless of unique events" in {
      val processor = OrderProcessor.testConcurrent[IO](10)
      val now = Instant.now
      val orders = List(
        OrderRow("1", "btc_eur", 100, 10, now, now),
        OrderRow("2", "btc_eur", 100, 20, now, now),
        OrderRow("1", "btc_eur", 100, 30, now, now),
        OrderRow("2", "btc_eur", 100, 20, now, now),
        OrderRow("3", "btc_eur", 100, 50, now, now),
        OrderRow("1", "btc_eur", 100, 60, now, now),
        OrderRow("5", "btc_eur", 100, 70, now, now),
        OrderRow("4", "btc_eur", 100, 80, now, now),
        OrderRow("2", "btc_eur", 100, 90, now, now)
      )

      for {
        currentlyProcessing <- Ref.of[IO, Int](0)
        maxConcurrencyRef <- Ref.of[IO, Int](0)
        processedRef <- Ref[IO].of(Vector.empty[OrderRow])
        processOrder = (order: OrderRow) =>
          for {
            _ <- currentlyProcessing.update(_ + 1)
            current <- currentlyProcessing.get
            _ <- maxConcurrencyRef.update(math.max(current, _))
            _ <- IO.sleep(200.milliseconds) // Simulate some processing time
            _ <- currentlyProcessing.update(_ - 1)
            _ <- processedRef.update(_ :+ order)
          } yield ()
        _ <- Stream.emits(orders)
          .covary[IO]
          .metered(10.milliseconds)
          .through(processor(processOrder))
          .compile
          .drain
        _ <- IO.sleep(200.milliseconds)
        processed <- processedRef.get
        maxConcurrency <- maxConcurrencyRef.get
      } yield {
        processed.size shouldBe 9
        maxConcurrency shouldBe 9
      }
    }

    "should process concurrently for different order ids equal to the configured limit" in {
      val processor = OrderProcessor.concurrent[IO](4)
      val now = Instant.now
      val orders = List(
        OrderRow("1", "btc_eur", 100, 10, now, now),
        OrderRow("2", "btc_eur", 100, 20, now, now),
        OrderRow("1", "btc_eur", 100, 30, now, now),
        OrderRow("2", "btc_eur", 100, 20, now, now),
        OrderRow("3", "btc_eur", 100, 50, now, now),
        OrderRow("1", "btc_eur", 100, 60, now, now),
        OrderRow("5", "btc_eur", 100, 70, now, now),
        OrderRow("4", "btc_eur", 100, 80, now, now),
        OrderRow("2", "btc_eur", 100, 90, now, now)
      )

      for {
        currentlyProcessing <- Ref.of[IO, Int](0)
        maxConcurrencyRef <- Ref.of[IO, Int](0)
        processedRef <- Ref[IO].of(Vector.empty[OrderRow])
        processOrder = (order: OrderRow) =>
          for {
            _ <- currentlyProcessing.update(_ + 1)
            current <- currentlyProcessing.get
            _ <- maxConcurrencyRef.update(math.max(current, _))
            _ <- IO.sleep(200.milliseconds) // Simulate some processing time
            _ <- currentlyProcessing.update(_ - 1)
            _ <- processedRef.update(_ :+ order)
          } yield ()
        _ <- Stream.emits(orders)
          .covary[IO]
          .metered(10.milliseconds)
          .through(processor(processOrder))
          .compile
          .drain
        _ <- IO.sleep(200.milliseconds)
        processed <- processedRef.get
        maxConcurrency <- maxConcurrencyRef.get
      } yield {
        processed.size shouldBe 9
        maxConcurrency shouldBe 4
      }
    }

    "should debounce rapid updates for the same OrderId" in {
      val processor = OrderProcessor.concurrent[IO](10)
      val now = Instant.now
      val orders = List(
        OrderRow("1", "btc_eur", 100, 10, now, now),
        OrderRow("1", "btc_eur", 100, 20, now, now),
        OrderRow("1", "btc_eur", 100, 30, now, now)
      )

      for {
        processedRef <- Ref[IO].of(Vector.empty[OrderRow])
        processOrder = (order: OrderRow) => processedRef.update(_ :+ order)
        _ <- Stream.emits(orders)
            .covary[IO]
            .metered(10.milliseconds)
            .through(processor(processOrder))
            .compile
            .drain
        _ <- IO.sleep(200.milliseconds)
        processed <- processedRef.get
      } yield processed.size shouldBe 3
    }

  }
}