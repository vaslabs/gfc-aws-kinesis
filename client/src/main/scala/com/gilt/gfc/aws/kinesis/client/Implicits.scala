package com.gilt.gfc.aws.kinesis.client

/**
 * Few implicit definitions for easy import.
 */
object Implicits {


  /** NOOP converter. */
  implicit object IdentityKinesisRecordReader extends KinesisRecordReader[ReadRecord] {
    override def apply(r: ReadRecord): ReadRecord = r
  }

  /** NOOP converter. */
  implicit object IdentityKinesisRecordWriter extends KinesisRecordWriter[WriteRecord] {
    override def apply(r: WriteRecord): WriteRecord = r
  }

}
