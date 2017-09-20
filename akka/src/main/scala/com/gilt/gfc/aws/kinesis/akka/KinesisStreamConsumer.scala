package com.gilt.gfc.aws.kinesis.akka

import com.gilt.gfc.aws.kinesis.client.{KCLConfiguration, KCLWorkerRunner, KinesisRecordReader}

class KinesisStreamConsumer[T](
  streamConfig: KinesisStreamConsumerConfig[T],
  handler: KinesisStreamHandler[T]
) (
  implicit private val evReader: KinesisRecordReader[T]
) {
  private val kclConfig = KCLConfiguration(
    streamConfig.applicationName,
    streamConfig.streamName,
    streamConfig.kinesisCredentialsProvider,
    streamConfig.dynamoCredentialsProvider,
    streamConfig.cloudWatchCredentialsProvider,
    streamConfig.regionName,
    streamConfig.initialPositionInStream,
    streamConfig.kinesisClientEndpoints,
    streamConfig.failoverTimeoutMillis
  )

  private val adapterConfig = streamConfig.dynamoDBKinesisAdapterClient.fold(kclConfig) {
    _ => kclConfig.withMaxRecords(1000) //using AWS recommended value
      .withIdleTimeBetweenReadsInMillis(500) //using AWS recommended value
  }

  private def createWorker = KCLWorkerRunner(
    adapterConfig,
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

