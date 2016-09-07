package com.gilt.gfc.aws.kinesis.akka

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record

trait KinesisStreamHandler[T] {
  def deserialize(r: Record): T
  def onRecord(shardId: String, message: T, checkpointer: IRecordProcessorCheckpointer): Unit
  def onInit(shardId: String): Unit = {}
  def onShutdown(shardId: String, checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason): Unit = {}
}

object KinesisStreamHandler {

  /**
    * @param deserialize Deserializer for the Kinesis messages
    * @param onRecord A method to call for each message received
    * @tparam T Type of the message
    * @return New kinesis stream handler object
    */
  def apply[T](
    deserialize: Array[Byte] => T,
    onRecord: (String, T, IRecordProcessorCheckpointer) => Unit
  ): KinesisStreamHandler[T] = {
    val underlyingOnRecord = onRecord
    val underlyingDeserialize = deserialize
    new KinesisStreamHandler[T] {
      override def deserialize(r: Record) = underlyingDeserialize(r.getData.array())
      override def onRecord(shardId: String, message: T, checkpointer: IRecordProcessorCheckpointer): Unit = underlyingOnRecord(shardId, message, checkpointer)
    }
  }
}