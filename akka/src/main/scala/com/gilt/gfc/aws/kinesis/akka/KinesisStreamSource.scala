package com.gilt.gfc.aws.kinesis.akka

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Source, SourceQueue}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object KinesisStreamSource {
  def pumpKinesisStreamTo[T](queue: SourceQueue[T], timeOutDuration: Duration = Duration.Inf): (String, T, IRecordProcessorCheckpointer) => Unit = {
    (sharId: String, message: T, checkpointer: IRecordProcessorCheckpointer) => Await.result(queue.offer(message), timeOutDuration)
  }

  def apply[T](
    streamConfig: KinesisStreamConsumerConfig[T],
    pumpingTimeoutDuration: Duration = Duration.Inf
  ) = {
    Source.queue[T](0, OverflowStrategy.backpressure)
      .mapMaterializedValue(queue => {
        val consumer = new KinesisStreamConsumer[T](streamConfig, KinesisStreamHandler(pumpKinesisStreamTo(queue, pumpingTimeoutDuration)))
        consumer.run
      })
  }
}
