package com.gilt.gfc.aws.kinesis.akka

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason

trait KinesisStreamHandler[T] {
  def onRecord(shardId: String, message: T, checkpointer: IRecordProcessorCheckpointer): Unit
  def onInit(shardId: String): Unit = {}
  def onShutdown(shardId: String, checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason): Unit = {}
}

object KinesisStreamHandler {
  def apply[T](onRecord: (String, T, IRecordProcessorCheckpointer) => Unit): KinesisStreamHandler[T] = {
    val _onRecord = onRecord
    new KinesisStreamHandler[T] {
      override def onRecord(shardId: String, message: T, checkpointer: IRecordProcessorCheckpointer): Unit = _onRecord(shardId, message, checkpointer)
    }
  }
}