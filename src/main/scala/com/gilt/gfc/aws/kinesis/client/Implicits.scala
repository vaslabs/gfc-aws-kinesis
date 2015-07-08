package com.gilt.gfc.aws.kinesis.client

import com.amazonaws.services.kinesis.model.Record

/**
 * Few implicit definitions for easy import.
 */
object Implicits {


  /** NOOP converter. */
  implicit
  object IdentityKinesisRecordReader
    extends KinesisRecordReader[Record] {

    override
    def apply(r: Record): Record = r
  }

}
