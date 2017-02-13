package com.gilt.gfc.aws.kinesis.akka

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Source, SourceQueue}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.gilt.gfc.aws.kinesis.client.KinesisRecordReader

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

object KinesisNonBlockingStreamSource {

  /**
    * Provides a way to pump Kinesis messages to materialized Akka stream
    * @param queue Materualized akka stream, which source is Source.queue[T]
    * @param timeOutDuration Maximum time for the akka stream to process messages
    * @return A message handler function to use in KinesisStreamHandler
    */
  def pumpKinesisStreamTo[T](queue: SourceQueue[T], timeOutDuration: Duration = Duration.Inf): (String, T, IRecordProcessorCheckpointer) => Unit = {
    (sharId: String, message: T, checkpointer: IRecordProcessorCheckpointer) => Await.result(queue.offer(message), timeOutDuration)
  }

  /**
    * Creates a non-materialized akka source connected to Kinesis stream
    * Upon materialization it will create a kinesis worker, and start consuming the Kinesis stream,
    * pumping messages to the underlying flow
    * @param streamConfig Configuration of the Kinesis stream to consume
    * @param pumpingTimeoutDuration Duration that source will wait for the akka stream to process message
    * @param evReader Deserialization typeclass
    * @return akka Source that on materialization gets messages from Kinesis stream and pump them through the flow
    */
  def apply[T](
    streamConfig: KinesisStreamConsumerConfig[T],
    pumpingTimeoutDuration: Duration = Duration.Inf,
    executionContext: ExecutionContext
  ) (
    implicit evReader: KinesisRecordReader[T]
  ) = {
    Source.queue[T](0, OverflowStrategy.backpressure)
      .mapMaterializedValue(queue => {
        val consumer = new KinesisStreamConsumer[T](streamConfig, KinesisStreamHandler(pumpKinesisStreamTo(queue, pumpingTimeoutDuration)))
        implicit val ec = executionContext
        Future(consumer.run) //this is a future so stream materialization will not block
      })
  }
}
