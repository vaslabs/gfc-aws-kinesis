package com.gilt.gfc.aws.kinesis.client

import com.amazonaws.services.kinesis.model.{PutRecordsRequestEntry, PutRecordsResultEntry}


/** Used to group putRecords call results.
  *
  * @param allResults each request entry paired up with server response, if any.
  * @param requestId AWS request ID, for logging / debugging
  */
private[client]
case class KinesisPublisherPutRecordsCallResults (
  allResults: Seq[( PutRecordsRequestEntry
                  , Option[PutRecordsResultEntry])]
, requestId: Option[String] = None
, responseHeaders: Map[String,String] = Map.empty // for debugging
) {

  /** Separates failed records from successfully published ones. */
  val (successes, failures) = allResults.partition {
    case (_, None)    => false // no result, call failed
    case (_, Some(r)) => r.getErrorCode == null
  }

  /** Splits failures into 2 cases: 'soft' errors (can be retried again with some hope of success) and 'hard' failures,
    * such as validation errors, which will presumably fail again for the same reason if we attempt to re-send. */
  val (softFailures, hardFailures) = failures.partition {
    case (_,None) => true // batch call failed completely, no record-specific response from kinesis
    case (_,Some(r)) => r.getErrorCode match { // http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html
      case "InternalFailure" => true
      case "ProvisionedThroughputExceededException" => true
      case "ServiceUnavailable" => true
      case _ => false
    }
  }

  /** Checks if we have any detailed per-record call results, if not - it's a generic error. */
  def isGenericServerError: Boolean = {
    failures.exists(_._2.isEmpty)
  }
}

