package com.gilt.gfc.aws.kinesis.client

/**
 * Simplified view of AWS SDK's kinesis record, just a couple of things we care about.
 */
case class KinesisRecord (
  partitionKey: String,
  data: Array[Byte]
)

/** 'type class' of things that can be converted to KinesisRecord. */
trait KinesisRecordWriter[R] {
  def toKinesisRecord(r: R): KinesisRecord
}
