package com.gilt.gfc.aws.kinesis.client

import com.amazonaws.services.kinesis.model.Record


/** Type class of things that kinesis records can be converted to. */
trait KinesisRecordReader[A] {
  def apply(r: Record): A
}

object KinesisRecordReader {
  def apply[A](deserializer: Record => A): KinesisRecordReader[A] = new KinesisRecordReader[A] {
    override def apply(r: Record): A = deserializer(r)
  }
}
