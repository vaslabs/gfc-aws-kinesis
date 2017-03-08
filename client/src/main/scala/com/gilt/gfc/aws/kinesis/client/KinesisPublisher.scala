package com.gilt.gfc.aws.kinesis.client

import java.nio.ByteBuffer
import java.util.concurrent._

import com.amazonaws.{ClientConfiguration, ClientConfigurationFactory}
import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.regions.Region
import com.amazonaws.retry.PredefinedRetryPolicies
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.{PutRecordsRequest, PutRecordsRequestEntry}
import com.gilt.gfc.concurrent.ThreadFactoryBuilder
import com.gilt.gfc.logging.Loggable

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Future, ExecutionContext, ExecutionContextExecutor}
import scala.language.postfixOps
import scala.util.Random
import scala.util.control.NonFatal


/** Stats about attempted call to publishBatch(), mostly to allow metrics library hooks. */
case class KinesisPublisherBatchResult(
  successRecordCount: Int = 0 // number of successfully published records
, failureRecordCount: Int = 0 // number of records we've failed to publish even after all retries
, serviceErrorCount: Int = 0  // number of failed attempts to publish a record
, attemptCount: Int = 0       // number of attempts to publish
) {
  require(successRecordCount >= 0, s"successRecordCount must be >= 0")
  require(failureRecordCount >= 0, s"failureRecordCount must be >= 0")
  require(serviceErrorCount  >= 0, s"serviceErrorCount must be >= 0")
  require(attemptCount       >= 0, s"attemptCount must be >= 0")

  def + ( other: KinesisPublisherBatchResult
        ): KinesisPublisherBatchResult = {

    KinesisPublisherBatchResult(
      successRecordCount = this.successRecordCount + other.successRecordCount
    , failureRecordCount = this.failureRecordCount + other.failureRecordCount
    , serviceErrorCount  = this.serviceErrorCount  + other.serviceErrorCount
    , attemptCount       = this.attemptCount       + other.attemptCount
    )
  }
}


/** Publishes a mini-batch of records to Kinesis. Implements the necessary retry logic. */
trait KinesisPublisher {

  /** Publishes record batch, asynchronously, this call returns immediately
    * and simply schedules a 'fire and forget' call to kinesis.
    */
  def publishBatch[R]( streamName: String
                     , records: Iterable[R]
                    )( implicit ev: KinesisRecordWriter[R]
                     ): Future[KinesisPublisherBatchResult]


  /** Shutdown background tasks.
    * @param awaitDuration how long to wait for the currently scheduled tasks to finish.
    */
  def shutdown( awaitDuration: FiniteDuration = 10 seconds
              ): Unit
}


/** Constructs KinesisPublisher. */
object KinesisPublisher {

  /** Constructs KinesisPublisher.
    *
    * @param maxErrorRetryCount       how many times to retry in the case of publishing errors
    * @param threadPoolSize           we make synchronous requests to kinesis, this determines parallelism
    * @param awsCredentialsProvider   override default credentials provider
    */
  def apply( maxErrorRetryCount: Int = 10
           , threadPoolSize: Int = 8
           , awsCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain()
           , awsRegion: Option[Region] = None
           , proxySettings: Option[ProxySettings] = None
           ): KinesisPublisher = new KinesisPublisherImpl(maxErrorRetryCount, threadPoolSize, awsCredentialsProvider, awsRegion, proxySettings)
}


private[client]
class KinesisPublisherImpl (
  maxErrorRetryCount: Int
, threadPoolSize: Int
, awsCredentialsProvider: AWSCredentialsProvider
, awsRegion: Option[Region]
, proxySettings: Option[ProxySettings]
) extends KinesisPublisher
     with Loggable {

  /** Publishes a batch of records to kinesis.
    *
    * @param streamName  kinesis stream
    * @param records     records (must have an associated implementation of KinesisRecordWriter)
    * @param krw         a writer implementation for submitted record type
    * @tparam R          record type
    * @return            Batch result with the call stats
    */
  override
  def publishBatch[R]( streamName: String
                     , records: Iterable[R]
                    )( implicit krw: KinesisRecordWriter[R]
                    ): Future[KinesisPublisherBatchResult] = {

    Future { // closes over input and adds a task to the thread pool
      try {
        putRecords(streamName, prepareRequestEntries(records).toSeq)
      } catch {
        case e: Throwable =>
          error(s"Kinesis call to publish batch to ${streamName} failed: ${e.getMessage}", e)
          KinesisPublisherBatchResult(failureRecordCount = records.size, attemptCount = 1, serviceErrorCount = 1)
      }
    }
  }


  override
  def shutdown( awaitDuration: FiniteDuration
              ): Unit = {

    executor.shutdown()
    executor.awaitTermination(awaitDuration.toMillis, TimeUnit.MILLISECONDS)
  }



  // Kinesis failures are usually "partial", some records get published while a few may fail.
  // AWS SDK built-in retry logic doesn't seem to pick that up.
  // Kinesis documented way to handle it is to get a list of failed records from a response
  // and retry them in your own code.
  @tailrec
  private[this]
  def putRecords( streamName: String
                , recordEntries: Seq[PutRecordsRequestEntry] // need an ordered collection here, matching results by index
                , batchResult: KinesisPublisherBatchResult = KinesisPublisherBatchResult()
                ): KinesisPublisherBatchResult = {
    debug(s"putRecords: streamName=${streamName}, recordEntries=${recordEntries.size}, batchResult=${batchResult}")

    if (batchResult.attemptCount > maxErrorRetryCount) {
      error(s"Failed to put ${recordEntries.size} records to ${streamName} after ${maxErrorRetryCount} unsuccessful retries!")

      batchResult + KinesisPublisherBatchResult(failureRecordCount = recordEntries.size)

    } else {

      if (batchResult.attemptCount > 0) { // back off a bit if we are retrying
        val sleepTimeMillis = 100 + Random.nextInt(3000)
        warn(s"Retrying to put ${recordEntries.size} records to ${streamName} after failure, retryAttempt=${batchResult.attemptCount}, sleeping for ${sleepTimeMillis} millis...")
        Thread.sleep(sleepTimeMillis)
      }

      val leftover = tryPutRecords(streamName, recordEntries)

      val thisResult = batchResult + KinesisPublisherBatchResult(
        successRecordCount = recordEntries.size - leftover.size
      , serviceErrorCount = leftover.size
      , attemptCount = 1
      )

      debug(s"Attempt ${thisResult.attemptCount}: published ${recordEntries.size} records to ${streamName} with ${leftover.size} left over due to errors")

      if (leftover.isEmpty) {
        thisResult
      } else {
        putRecords(streamName, leftover, thisResult)
      }
    }
  }


  /** Tries to put a batch of records.
    * Returns a subset of records that weren't published.
    * Most common reason for that is the 500 service error from kinesis which (in small numbers) is said to be expected.
    *
    * @param streamName      kinesis stream name
    * @param recordEntries   records to be published
    * @return                records that we failed to publish
    */
  private[this]
  def tryPutRecords( streamName: String
                   , recordEntries: Seq[PutRecordsRequestEntry] // need an ordered collection here, matching results by index
                   ): Seq[PutRecordsRequestEntry]  = {

    // API is very imperative, see "Example PutRecords failure handler"
    // http://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-sdk.html#kinesis-using-sdk-java-putrecords
    // one is supposed to match arrays by index, at least they seem to be always of the same size

    try {
      val res = kinesisClient.putRecords(
        new PutRecordsRequest().
          withStreamName(streamName).
          withRecords(recordEntries.asJavaCollection)
      )

      if (res.getFailedRecordCount > 0) {
        val failedRecords = for (
          (res,req) <- res.getRecords.asScala.zip(recordEntries) if res.getErrorCode != null
        ) yield {
          req
        }

        failedRecords

      } else {

        Seq.empty
      }

    } catch {
      case NonFatal(e) =>
        warn(s"Kinesis call to publish batch to ${streamName} failed: ${e.getMessage}", e)
        recordEntries
    }
  }


  /** N.B. AWS provides async client as well but it's just a simple wrapper around
    * sync client, with java-based Futures and other async primitives.
    * No reason to use it, really.
    */
  private[this]
  val kinesisClient = {
    val cf = new ClientConfigurationFactory()

    // There are intermittent 500 errors, the documented action is to retry
    val rp = PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(maxErrorRetryCount)
    val conf = cf.getConfig.withRetryPolicy(rp)

    proxySettings.foreach(_.configureClientConfiguration(conf))

    val client = new AmazonKinesisClient(awsCredentialsProvider, conf)
    awsRegion.foreach(region => client.setRegion(region))
    client
  }


  private[this]
  val executor = new java.util.concurrent.ThreadPoolExecutor(
    0
  , threadPoolSize
  , 30L
  , TimeUnit.SECONDS
  , new SynchronousQueue[Runnable]()
  , ThreadFactoryBuilder(getClass.getSimpleName, getClass.getSimpleName).build()
  )


  implicit
  private
  val executionContext: ExecutionContext = ExecutionContext.fromExecutor(executor, (e) => error(e.getMessage, e))


  private[this]
  def prepareRequestEntries[R]( records: Iterable[R]
                             )( implicit krw: KinesisRecordWriter[R]
                              ): Iterable[PutRecordsRequestEntry] = {
    for ( r <- records ) yield {

      val KinesisRecord(partitionKey, data) = krw.toKinesisRecord(r)
      val bb = ByteBuffer.wrap(data, 0, data.length)

      new PutRecordsRequestEntry().
        withData(bb).
        withPartitionKey(partitionKey)
    }
  }
}
