package com.gilt.gfc.aws.kinesis.client

import java.nio.ByteBuffer
import java.util.concurrent._

import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.{PutRecordsResult, PutRecordsRequest, PutRecordsRequestEntry}
import com.gilt.gfc.concurrent.ThreadFactoryBuilder
import com.gilt.gfc.logging.Loggable

import scala.concurrent.{Future, ExecutionContext}

/** Simple wrapper around functions we intend to use from AWS SDK. */
trait KinesisPublisher {

  /** Publishes record batch, asynchronously, this call returns immediately
    * and simply schedules a 'fire and forget' call to kinesis.
    */
  def publishBatch[R](streamName: String,
                      records: Iterable[R])
                     (implicit ev: KinesisRecordWriter[R])
                     : Future[PutRecordsResult]

}


object KinesisPublisher
  extends KinesisPublisher with Loggable {

  /** Publishes a batch of records to kinesis.
    *
    * @param streamName  kinesis stream
    * @param records     records (must have an associated implementation of KinesisRecordWriter)
    * @param krw         a writer implementation for submitted record type
    * @tparam R          record type
    * @return            Nothing, this is a 'fire and forget' call, we assume events are 'lossy', errors are logged.
    */
  override
  def publishBatch[R](streamName: String,
                      records: Iterable[R])
                     (implicit krw: KinesisRecordWriter[R])
                     : Future[PutRecordsResult] = {
    Future {
      kinesisClient.putRecords(prepareRequest(streamName, records))
    }
  }

  /** N.B. AWS provides async client as well but it's just a simple wrapper around
    * sync client, with java-based Futures and other async primitives.
    * No reason to use it, really.
    */
  private[this]
  val kinesisClient = new AmazonKinesisClient()

  implicit
  private[this]
  val executionContext = {
    val tpe = new java.util.concurrent.ThreadPoolExecutor(
      0,
      128, // don't let it go too crazy but we need large-ish number of threads because client is blocking
      30L, TimeUnit.SECONDS,
      new SynchronousQueue[Runnable](),
      ThreadFactoryBuilder(getClass.getSimpleName, getClass.getSimpleName).build()
    )

    ExecutionContext.fromExecutor(tpe, (e) => error(e.getMessage, e))
  }


  private[this]
  def prepareRequest[R](streamName: String,
                        records: Iterable[R])
                       (implicit krw: KinesisRecordWriter[R])
                       : PutRecordsRequest = {

    import scala.collection.JavaConverters._

    val putRecordsRequestEntries = for ( r <- records ) yield {

      val KinesisRecord(partitionKey, data) = krw.toKinesisRecord(r)
      val bb = ByteBuffer.wrap(data, 0, data.length)

      new PutRecordsRequestEntry().
        withData(bb).
        withPartitionKey(partitionKey)
    }

    new PutRecordsRequest().
      withStreamName(streamName).
      withRecords(putRecordsRequestEntries.asJavaCollection)
  }
}
