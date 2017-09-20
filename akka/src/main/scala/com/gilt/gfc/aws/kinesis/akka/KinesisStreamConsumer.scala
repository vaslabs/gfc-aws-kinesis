package com.gilt.gfc.aws.kinesis.akka

import com.gilt.gfc.aws.kinesis.client.{KCLConfiguration, KCLWorkerRunner, KinesisRecordReader}

import scala.concurrent.duration._

class KinesisStreamConsumer[T](
  streamConfig: KinesisStreamConsumerConfig[T],
  handler: KinesisStreamHandler[T]
) (
  implicit private val evReader: KinesisRecordReader[T]
) {

  val maxRecords: Option[Int] = streamConfig.maximumNumberOfRecordsToFetchOnEachCall.orElse(
    streamConfig.dynamoDBKinesisAdapterClient.flatMap(_ => Some(1000))
  )

  val idleTimeout: Option[FiniteDuration] = streamConfig.idleTimeBetweenReads.orElse(
    streamConfig.dynamoDBKinesisAdapterClient.flatMap(_ => Some(500 milliseconds))
  )

  private val kclConfig = KCLConfiguration(
    streamConfig.applicationName,
    streamConfig.streamName,
    streamConfig.kinesisCredentialsProvider,
    streamConfig.dynamoCredentialsProvider,
    streamConfig.cloudWatchCredentialsProvider,
    streamConfig.regionName,
    streamConfig.initialPositionInStream,
    streamConfig.kinesisClientEndpoints,
    streamConfig.failoverTimeoutMillis,
    maxRecords,
    idleTimeout
  )

  private def createWorker = KCLWorkerRunner(
    kclConfig,
    dynamoDBKinesisAdapter = streamConfig.dynamoDBKinesisAdapterClient,
    metricsFactory = Some(streamConfig.metricsFactory),
    checkpointInterval = streamConfig.checkPointInterval,
    initialize = handler.onInit,
    shutdown = handler.onShutdown,
    initialRetryDelay = streamConfig.retryConfig.initialDelay,
    maxRetryDelay = streamConfig.retryConfig.retryDelay,
    numRetries = streamConfig.retryConfig.maxRetries
  )

  /***
    * Creates the worker and runs it
    */
  def run() = {
    val worker = createWorker
    worker.runSingleRecordProcessor(handler.onRecord)
  }
}

