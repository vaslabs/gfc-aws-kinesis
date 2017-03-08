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
    streamConfig.proxySettings
  )

  private def createWorker = KCLWorkerRunner(
    kclConfig,
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

