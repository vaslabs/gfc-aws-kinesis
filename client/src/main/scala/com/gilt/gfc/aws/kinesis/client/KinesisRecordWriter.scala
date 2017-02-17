package com.gilt.gfc.aws.kinesis.client

/** 'type class' of things that can be converted to KinesisRecord. */
trait KinesisRecordWriter[R] {
  def apply(r: R): WriteRecord
}

object KinesisRecordWriter {
  def apply[R](serializer: R => WriteRecord): KinesisRecordWriter[R] = new KinesisRecordWriter[R] {
    override def apply(r: R): WriteRecord = serializer(r)
  }
}
