package com.gilt.gfc.aws.kinesis.client

import java.nio.ByteBuffer

import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry

/**
 * Simplified view of AWS SDK's kinesis record, just a couple of things we care about.
 */
case class WriteRecord (
  partitionKey: String,
  data: Array[Byte]
) {
  lazy val underlying: PutRecordsRequestEntry = {
    val bb = ByteBuffer.wrap(data)
    new PutRecordsRequestEntry().withData(bb).withPartitionKey(partitionKey)
  }
}
