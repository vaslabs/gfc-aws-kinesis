package com.gilt.gfc.aws.kinesis.client

import com.amazonaws.services.kinesis.model.Record

/**
  * Simplified view of AWS SDK's kinesis record, just a couple of things we care about.
  */
case class ReadRecord (underlying: Record) {

  lazy val data: Array[Byte] = ByteBufferUtil.toByteArray(underlying.getData)

  def partitionKey = underlying.getPartitionKey

}
