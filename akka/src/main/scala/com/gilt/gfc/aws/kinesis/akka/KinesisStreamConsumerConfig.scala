package com.gilt.gfc.aws.kinesis.akka

import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory
import com.gilt.gfc.aws.kinesis.client.KinesisClientEndpoints

import scala.concurrent.duration._

case class KinesisStreamConsumerConfig[T](
  streamName: String,
  applicationName: String,
  kinesisCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain(),
  dynamoCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain(),
  cloudWatchCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain(),
  metricsFactory: IMetricsFactory = new NullMetricsFactory(),
  checkPointInterval: FiniteDuration = 5.minutes,
  retryConfig: RetryConfig = RetryConfig(1.second, 1.second, 3),
  initialPositionInStream: InitialPositionInStream = InitialPositionInStream.LATEST,
  regionName: Option[String] = None,
  dynamoDBKinesisAdapterClient: Option[AmazonDynamoDBStreamsAdapterClient] = None,
  kinesisClientEndpoints: Option[KinesisClientEndpoints] = None,
  failoverTimeoutMillis: Option[Long] = None,
  maxRecordsPerBatch: Option[Int] = None,
  idleTimeBetweenReads: Option[FiniteDuration] = None
) {

  /**
    * Returns a config where kinesisCredentialsProvider, dynamoCredentialsProvider and cloudWatchCredentialsProvider
    * assigned the given value
    * @param credentialsProvider provider to use
    * @return New config object
    */
  def withCommonCredentialsProvider(credentialsProvider: AWSCredentialsProvider): KinesisStreamConsumerConfig[T] =
    this.copy(
      kinesisCredentialsProvider = credentialsProvider,
      dynamoCredentialsProvider = credentialsProvider,
      cloudWatchCredentialsProvider = credentialsProvider
    )
}
