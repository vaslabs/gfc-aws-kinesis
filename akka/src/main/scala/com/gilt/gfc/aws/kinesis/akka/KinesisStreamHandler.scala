package com.gilt.gfc.aws.kinesis.akka

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.model.Record
import com.gilt.gfc.aws.kinesis.client.KinesisRecordReader

trait KinesisStreamHandler[T] {
  def onRecord(shardId: String, message: T, checkpointer: IRecordProcessorCheckpointer)(implicit ev: KinesisRecordReader[T]): Unit
  def onInit(shardId: String): Unit = {}
  def onShutdown(shardId: String, checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason): Unit = {}
}

object KinesisStreamHandler {

  /**
    * @param onRecord A method to call for each message received
    * @param evReader Deserialization typeclass
    * @tparam T Type of the message
    * @return New kinesis stream handler object
    */
  def apply[T](
    onRecord: (String, T, IRecordProcessorCheckpointer) => Unit
  )(
    implicit evReader: KinesisRecordReader[T]
  ): KinesisStreamHandler[T] = {
    val underlyingOnRecord = onRecord
    new KinesisStreamHandler[T] {
      override def onRecord(shardId: String, message: T, checkpointer: IRecordProcessorCheckpointer)(implicit ev: KinesisRecordReader[T]): Unit =
        underlyingOnRecord(shardId, message, checkpointer)
    }
  }
}
