package com.gilt.gfc.aws.kinesis.client

import java.util.UUID

import com.amazonaws.services.kinesis.model.{PutRecordsRequestEntry, PutRecordsResultEntry}
import org.specs2.mutable.Specification

class KinesisPublisherPutRecordsCallResultsTest
  extends Specification {

  "KinesisPublisherPutRecordsCallResults" should {
    "handle empty results" in {
      val r = KinesisPublisherPutRecordsCallResults(Seq.empty)

      r.successes.isEmpty must_===(true)
      r.failures.isEmpty must_===(true)
      r.softFailures.isEmpty must_===(true)
      r.hardFailures.isEmpty must_===(true)
      r.isGenericServerError must_===(false)
    }


    "handle generic service errors" in {
      val r = KinesisPublisherPutRecordsCallResults(Seq(
        e -> None
      , e -> None
      ))

      r.successes.isEmpty must_===(true)
      r.failures.size must_===(2)
      r.softFailures.size must_===(2)
      r.hardFailures.isEmpty must_===(true)
      r.isGenericServerError must_===(true)
    }


    "handle successful calls" in {
      val r = KinesisPublisherPutRecordsCallResults(Seq(
        e -> re(null)
      , e -> re(null)
      ))

      r.successes.size must_===(2)
      r.failures.isEmpty must_===(true)
      r.softFailures.isEmpty must_===(true)
      r.hardFailures.isEmpty must_===(true)
      r.isGenericServerError must_===(false)
    }

    "handle partial batch failures" in {
      val r = KinesisPublisherPutRecordsCallResults(Seq(
        e -> re(null) // success
      , e -> re("blahblah") // 'hard' error, no retry
      , e -> re("foobar") // 'hard' error, no retry
      , e -> re("InternalFailure") // 'soft' error, should retry
      ))

      r.successes.size must_===(1)
      r.failures.size must_===(3)
      r.softFailures.size must_===(1)
      r.hardFailures.size must_===(2)
      r.isGenericServerError must_===(false)
    }
  }


  def e: PutRecordsRequestEntry = {
    new PutRecordsRequestEntry().withPartitionKey(UUID.randomUUID.toString)
  }

  def re( errCode: String
        ): Option[PutRecordsResultEntry] = {
    Some(new PutRecordsResultEntry().withErrorCode(errCode))
  }
}
