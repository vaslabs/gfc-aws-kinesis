package com.gilt.gfc.aws.kinesis.client


import scala.collection.immutable.HashMap
import scala.language.postfixOps


/** Stats about attempted call to publishBatch(), mostly to allow metrics library hooks. */
case class KinesisPublisherBatchResult(
  successRecordCount: Int = 0 // number of successfully published records
, failureRecordCount: Int = 0 // number of records we've failed to publish even after all retries
, serviceErrorCount: Int = 0  // number of failed attempts to publish a record
, attemptCount: Int = 0       // number of attempts to publish
, errorCodes: HashMap[String, Int] = HashMap.empty // Reported error codes, see http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html
, shardRecordCounts: HashMap[String, Int] = HashMap.empty // Counts of records by shard ID, to aid in debugging sharding problems (skipped from toString representation)
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
    , errorCodes         = this.errorCodes.merged(other.errorCodes)({case ((k,v1),(_,v2)) => (k,v1+v2) })
    , shardRecordCounts  = this.shardRecordCounts.merged(other.shardRecordCounts)({case ((k,v1),(_,v2)) => (k,v1+v2) })
    )
  }

  /** Prints field names and hides shardRecordCounts (which is too verbose) to make it easier to read the logs. */
  override
  lazy val toString: String = {
    s"KinesisPublisherBatchResult(attemptCount=${attemptCount}, successRecordCount=${successRecordCount}, failureRecordCount=${failureRecordCount}, serviceErrorCount=${serviceErrorCount}, errorCodes=${errorCodes})"
  }
}


object KinesisPublisherBatchResult {

  /** Summarizes kinesis call results. */
  private[client]
  def apply( callResults: KinesisPublisherPutRecordsCallResults
           ): KinesisPublisherBatchResult = {

    val failedResultEntries = callResults.failures.map(_._2).flatten
    val errorCodes = failedResultEntries.map(_.getErrorCode).groupBy(identity _).mapValues(_.size).toSeq

    val shardRecordCounts =
      callResults.successes.map(_._2).flatten.
      map(r => Option(r.getShardId)).flatten.
      groupBy(identity _).mapValues(_.size).toSeq

    KinesisPublisherBatchResult(
      successRecordCount = callResults.successes.size
    , failureRecordCount = callResults.hardFailures.size
    , serviceErrorCount = if (callResults.isGenericServerError) 1 else 0
    , attemptCount = 1
    , errorCodes = HashMap(errorCodes: _*)
    , shardRecordCounts = HashMap(shardRecordCounts: _*)
    )
  }
}
