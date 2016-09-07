package com.gilt.gfc.aws.kinesis.akka

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Source, SourceQueue}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object KinesisStreamSource {

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
    * @param deserializer Deserializer function for messages
    * @param pumpingTimeoutDuration Duration that source will wait for the akka stream to process message
    */
  def apply[T](
    streamConfig: KinesisStreamConsumerConfig[T],
    deserializer: Array[Byte] => T,
    pumpingTimeoutDuration: Duration = Duration.Inf
  ) = {
    Source.queue[T](0, OverflowStrategy.backpressure)
      .mapMaterializedValue(queue => {
        val consumer = new KinesisStreamConsumer[T](streamConfig, KinesisStreamHandler(deserializer, pumpKinesisStreamTo(queue, pumpingTimeoutDuration)))
        consumer.run
      })
  }
}
