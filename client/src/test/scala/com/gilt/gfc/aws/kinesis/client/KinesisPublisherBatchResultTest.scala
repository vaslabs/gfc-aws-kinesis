package com.gilt.gfc.aws.kinesis.client

import java.util.UUID

import com.amazonaws.services.kinesis.model.{PutRecordsRequestEntry, PutRecordsResultEntry}
import org.specs2.mutable.Specification

import scala.collection.immutable.HashMap


class KinesisPublisherBatchResultTest
  extends Specification {


  "KinesisPublisherBatchResult" should {
    "be additive" in {
      (KinesisPublisherBatchResult() + KinesisPublisherBatchResult()) should_===(KinesisPublisherBatchResult())

      (KinesisPublisherBatchResult(
        successRecordCount = 1
      , failureRecordCount = 10
      , serviceErrorCount = 100
      , attemptCount = 1000
      , errorCodes = HashMap("foo" -> 1, "bar" -> 2)
      ) + KinesisPublisherBatchResult(
        successRecordCount = 2
      , failureRecordCount = 20
      , serviceErrorCount = 200
      , attemptCount = 2000
      , errorCodes = HashMap(("foo" -> 3))
      )) should_===(KinesisPublisherBatchResult(
        successRecordCount = 3
      , failureRecordCount = 30
      , serviceErrorCount = 300
      , attemptCount = 3000
      , errorCodes = HashMap("foo" -> 4, "bar" -> 2)
      ))
    }


    "count errors" in {

      KinesisPublisherBatchResult(KinesisPublisherPutRecordsCallResults(Seq.empty)) should_===(KinesisPublisherBatchResult(
        successRecordCount = 0
      , serviceErrorCount = 0
      , attemptCount = 1
      ))

      KinesisPublisherBatchResult(KinesisPublisherPutRecordsCallResults(Seq((r,None)))) should_===(KinesisPublisherBatchResult(
        successRecordCount = 0
      , failureRecordCount = 0
      , serviceErrorCount = 1
      , attemptCount = 1
      ))

      KinesisPublisherBatchResult(KinesisPublisherPutRecordsCallResults(
        (Seq.tabulate(4)(i => cr(s"shard${i}", null)) ) ++
        (for {
          errCode <- Seq("err1", "err2", "ServiceUnavailable") // ServiceUnavailable is a recoverable error
        } yield (cr(null, errCode)))
      )) should_===(KinesisPublisherBatchResult(
        successRecordCount = 4
      , failureRecordCount = 2 // err1 and err2 are not to be retried
      , attemptCount = 1
      , errorCodes = HashMap("err2" -> 1, "ServiceUnavailable" -> 1, "err1" -> 1)
      , shardRecordCounts = HashMap("shard0" -> 1, "shard1" -> 1, "shard2" -> 1, "shard3" -> 1)
      ))

      // generic internal error, no shard data
      KinesisPublisherBatchResult(KinesisPublisherPutRecordsCallResults(
        (Seq.tabulate(4)(i => cr(s"shard${i}", null)) ) ++
        (for {
          errCode <- Seq("err1", "err2", "err3")
        } yield (cr(null, errCode)))
      )) should_===(KinesisPublisherBatchResult(
        successRecordCount = 4
      , failureRecordCount = 3
      , attemptCount = 1
      , errorCodes = HashMap("err2" -> 1, "err3" -> 1, "err1" -> 1)
      , shardRecordCounts = HashMap("shard0" -> 1, "shard1" -> 1, "shard2" -> 1, "shard3" -> 1)
      ))
    }


    "count shard collisions" in {

      KinesisPublisherBatchResult(KinesisPublisherPutRecordsCallResults(
        ( Seq.fill(3)(cr(s"shard_1", null)) ) ++
        ( Seq.fill(4)(cr(s"shard_2", null)) )
      )) should_===(KinesisPublisherBatchResult(
        successRecordCount = 7
      , failureRecordCount = 0
      , attemptCount = 1
      , errorCodes = HashMap.empty
      , shardRecordCounts = HashMap("shard_1" -> 3, "shard_2" -> 4)
      ))
    }
  }

  def r: PutRecordsRequestEntry = {
    new PutRecordsRequestEntry().withPartitionKey(UUID.randomUUID.toString)
  }

  def cr( shardId: String
        , errCode: String
        ): (PutRecordsRequestEntry, Option[PutRecordsResultEntry]) = {
    (r,Some(new PutRecordsResultEntry().withErrorCode(errCode).withShardId(shardId)))
  }
}
