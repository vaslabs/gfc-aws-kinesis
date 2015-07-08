package com.gilt.gfc.aws.kinesis.client

import com.amazonaws.services.kinesis.model.Record


/** Type class of things that kinesis records can be converted to. */
trait KinesisRecordReader[A] {
  def apply(r: Record): A
}
