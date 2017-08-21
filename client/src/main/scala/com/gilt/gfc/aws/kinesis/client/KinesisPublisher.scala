package com.gilt.gfc.aws.kinesis.client

import java.nio.ByteBuffer
import java.util.concurrent._

import com.amazonaws.ClientConfigurationFactory
import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Region
import com.amazonaws.retry.PredefinedRetryPolicies
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.model.{PutRecordsRequest, PutRecordsRequestEntry}
import com.gilt.gfc.concurrent.ThreadFactoryBuilder
import com.gilt.gfc.logging.Loggable

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.language.postfixOps
import scala.util.Random
import scala.util.control.NonFatal


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
    * @param awsRegion                override default region
    * @param awsEndpointConfig        override default endpoint
    */
  def apply( maxErrorRetryCount: Int = 10
           , threadPoolSize: Int = 8
           , awsCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain()
           , awsRegion: Option[Region] = None
           , awsEndpointConfig: Option[EndpointConfiguration] = None
           ): KinesisPublisher = {

    new KinesisPublisherImpl(
      maxErrorRetryCount
    , newDefaultExecutor(threadPoolSize)
    , awsCredentialsProvider
    , awsRegion
    , awsEndpointConfig
    )
  }


  /** Constructs KinesisPublisher with a custom executor.
    *
    * @param maxErrorRetryCount       how many times to retry in the case of publishing errors
    * @param executor                 custom executor service
    * @param awsCredentialsProvider   override default credentials provider
    * @param awsRegion                override default region
    */
  def apply( maxErrorRetryCount: Int
           , executor: ExecutorService
           , awsCredentialsProvider: AWSCredentialsProvider
           , awsRegion: Option[Region]
           ): KinesisPublisher = {
    apply(maxErrorRetryCount, executor, awsCredentialsProvider, awsRegion, None)
  }

  /** Constructs KinesisPublisher with a custom executor.
    *
    * @param maxErrorRetryCount       how many times to retry in the case of publishing errors
    * @param executor                 custom executor service
    * @param awsCredentialsProvider   override default credentials provider
    * @param awsRegion                override default region
    * @param awsEndpointConfig        override default endpoint
    */
  def apply( maxErrorRetryCount: Int
           , executor: ExecutorService
           , awsCredentialsProvider: AWSCredentialsProvider
           , awsRegion: Option[Region]
           , awsEndpointConfig: Option[EndpointConfiguration]
           ): KinesisPublisher = {

    new KinesisPublisherImpl(
      maxErrorRetryCount
    , executor
    , awsCredentialsProvider
    , awsRegion
    , awsEndpointConfig
    )
  }


  /** Like Executors.newCachedThreadPool() but with both, thread factory and max size provided. */
  private[this]
  def newDefaultExecutor( threadPoolSize: Int
                        ): ExecutorService = {

    new ThreadPoolExecutor(
      0
    , threadPoolSize
    , 30L
    , TimeUnit.SECONDS
    , new SynchronousQueue[Runnable]()
    , ThreadFactoryBuilder(getClass.getSimpleName, getClass.getSimpleName).build()
    )
  }
}


private[client]
class KinesisPublisherImpl (
  maxErrorRetryCount: Int
, executor: ExecutorService
, awsCredentialsProvider: AWSCredentialsProvider
, awsRegion: Option[Region]
, awsEndpointConfig: Option[EndpointConfiguration]
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

    if (recordEntries.isEmpty) {
      warn(s"Skipping empty record batch ...")
      batchResult

    } else if (batchResult.attemptCount > maxErrorRetryCount) {
      error(s"Failed to put ${recordEntries.size} records to ${streamName} after ${maxErrorRetryCount} unsuccessful retries!")
      batchResult + KinesisPublisherBatchResult(failureRecordCount = recordEntries.size)

    } else {

      if (batchResult.attemptCount > 0) { // back off a bit if we are retrying
        val sleepTimeMillis = 100 + Random.nextInt(3000)
        warn(s"Retrying to put ${recordEntries.size} records to ${streamName} after failure, retryAttempt=${batchResult.attemptCount}, sleeping for ${sleepTimeMillis} millis...")
        Thread.sleep(sleepTimeMillis)
      }

      val callResults = tryPutRecords(streamName, recordEntries)
      val thisResult = KinesisPublisherBatchResult(callResults)
      val cumulativeResult = batchResult + thisResult
      val logPrefix = s"${streamName} attempt ${cumulativeResult.attemptCount}, req ID ${callResults.requestId}:"

      if (callResults.failures.isEmpty) {
        debug(s"${logPrefix} successfully published ${recordEntries.size} records")
      } else {
        warn(s"${logPrefix} published ${recordEntries.size} records with ${thisResult.failureRecordCount} left over due to errors: by errorCode: ${thisResult.errorCodes}, by shard: ${thisResult.shardErrors}")
      }

      callResults.hardFailures.foreach { case (e,r) =>
        error(s"${logPrefix} skipping ${e}, shard ID ${r.map(_.getShardId)} due to unrecoverable error ${r.map(_.getErrorCode)} ...")
      }

      if (callResults.softFailures.isEmpty) {
        cumulativeResult
      } else {
        putRecords(streamName, callResults.softFailures.map(_._1), cumulativeResult)
      }
    }
  }


  /** Tries to put a batch of records.
    *
    * @param streamName      kinesis stream name
    * @param recordEntries   records to be published
    * @return                request entries paired up with kinesis responses, if any
    */
  private[this]
  def tryPutRecords( streamName: String
                   , recordEntries: Seq[PutRecordsRequestEntry] // need an ordered collection here, matching results by index
                   ): KinesisPublisherPutRecordsCallResults = {
    try {
      val callRes = blocking {
        kinesisClient.putRecords(
          new PutRecordsRequest().
            withStreamName(streamName).
            withRecords(recordEntries.asJavaCollection)
        )
      }

      KinesisPublisherPutRecordsCallResults(
        allResults = for (
          (res,req) <- callRes.getRecords.asScala.zip(recordEntries)
        ) yield {
          (req, Some(res))
        }
      , requestId = Option(callRes.getSdkResponseMetadata).flatMap(m => Option(m.getRequestId))
      )
    } catch {
      case NonFatal(e) =>
        warn(s"Kinesis call to publish batch to ${streamName} failed: ${e.getMessage}", e)
        KinesisPublisherPutRecordsCallResults(recordEntries.map((_, None)))
    }
  }


  /** N.B. AWS provides async client as well but it's just a simple wrapper around
    * sync client, with java-based Futures and other async primitives.
    * No reason to use it, really, we can get Scala futures from Scala's execution context.
    */
  private[this]
  val kinesisClient = {
    val cf = new ClientConfigurationFactory()

    // There are intermittent 500 errors, the documented action is to retry
    val rp = PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(maxErrorRetryCount)
    val conf = cf.getConfig.withRetryPolicy(rp)

    val builder = AmazonKinesisClientBuilder.standard()
      .withClientConfiguration(conf)
      .withCredentials(awsCredentialsProvider)

    awsRegion.foreach(region => builder.setRegion(region.getName))
    awsEndpointConfig.foreach(endpoint => builder.setEndpointConfiguration(endpoint))

    builder.build()
  }


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
