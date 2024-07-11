package com.example

import cats.effect._
import cats.effect.testing.scalatest.AsyncIOSpec
import com.example.model.OrderRow
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
        processOrder = (order: OrderRow) => IO.sleep(10.millis) *> processedOrders.update(_ :+ order)
        processor = OrderProcessor[IO](OrderProcessor.ProcessingStrategy.Sequential)
        orders = List(
          OrderRow("1", "btc_eur", 50, 10, Instant.now, Instant.now),
          OrderRow("2", "btc_eur", 50, 20, Instant.now, Instant.now),
          OrderRow("1", "btc_eur", 50, 30, Instant.now, Instant.now),
          OrderRow("3", "btc_eur", 50, 40, Instant.now, Instant.now),
          OrderRow("2", "btc_eur", 50, 50, Instant.now, Instant.now)
        )
        _ <- processor.process(Stream.emits(orders), processOrder).compile.drain
        result <- processedOrders.get
      } yield {
        result mustBe orders
      }
    }

    "process orders concurrently but maintain order for same orderId" in {
      for {
        processedOrders <- Ref[IO].of(Vector.empty[OrderRow])
        currentlyProcessing <- Ref[IO].of(Set.empty[String])
        processor = OrderProcessor[IO](OrderProcessor.ProcessingStrategy.Concurrent(3))
        processOrder = (order: OrderRow) =>
          for {
            _ <- currentlyProcessing.update(_ + order.orderId)
            _ <- currentlyProcessing.get
            _ <- IO.sleep(50.millis) // Simulate some processing time
            _ <- processedOrders.update(_ :+ order)
            _ <- currentlyProcessing.update(_ - order.orderId)
          } yield ()
        orders = List(
          OrderRow("1", "btc_eur", 50, 10, Instant.now, Instant.now),
          OrderRow("2", "btc_eur", 50, 20, Instant.now, Instant.now),
          OrderRow("1", "btc_eur", 50, 30, Instant.now, Instant.now),
          OrderRow("3", "btc_eur", 50, 40, Instant.now, Instant.now),
          OrderRow("2", "btc_eur", 50, 50, Instant.now, Instant.now)
        )
        _ <- processor.process(Stream.emits(orders), processOrder).compile.drain
        result <- processedOrders.get
        maxConcurrent <- currentlyProcessing.get.map(_.size)
      } yield {
        // Check that all orders were processed
        result.size mustBe orders.size

        // Check that orders with the same ID are processed in order
        result.filter(_.orderId == "1").map(_.filled) mustBe Vector(10, 30)
        result.filter(_.orderId == "2").map(_.filled) mustBe Vector(20, 50)

        // Check that concurrent processing occurred
        maxConcurrent mustBe >=(2)

        // Check that the final result contains all orders
        result.toSet mustBe orders.toSet
      }
    }

    "handle a large number of orders efficiently" in {
      val numOrders = 500
      val maxConcurrent = 10

      for {
        processedOrders <- Ref[IO].of(Vector.empty[OrderRow])
        activeOrders <- Ref[IO].of(Set.empty[OrderRow])
        maxConcurrentRef <- Ref[IO].of(0)
        processor = OrderProcessor[IO](OrderProcessor.ProcessingStrategy.Concurrent(maxConcurrent))
        processOrder = (order: OrderRow) =>
          for {
            _ <- activeOrders.update(_ + order)
            _ <- IO.sleep(10.millis) // Simulate some processing time
            _ <- processedOrders.update(_ :+ order)
            _ <- activeOrders.update(_ - order)
            _ <- activeOrders.get.flatMap { currentSet =>
                  maxConcurrentRef.update(max => math.max(max, currentSet.size))
                }
          } yield ()
        orders = (1 to numOrders).map(i =>
                    OrderRow(s"order-${i % 50}", "btc_eur", 50, i, Instant.now, Instant.now)
                  ).toList
        start <- IO.monotonic
        _ <- processor.process(Stream.emits(orders), processOrder).compile.drain
        end <- IO.monotonic
        result <- processedOrders.get
        maxConcurrent <- maxConcurrentRef.get
      } yield {
        result.size mustBe numOrders
        maxConcurrent mustBe >(1)
        maxConcurrent mustBe <=(10)
        (end - start).toMillis mustBe <(5.seconds.toMillis)
      }
    }
  }
}