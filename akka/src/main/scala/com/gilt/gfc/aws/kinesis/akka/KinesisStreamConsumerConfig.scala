package com.gilt.gfc.aws.kinesis.akka

import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory

import scala.concurrent.duration._

case class KinesisStreamConsumerConfig[T](
  streamName: String,
  applicationName: String,
  recordDeserializer: Array[Byte] => T,
  kinesisCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain(),
  dynamoCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain(),
  cloudWatchCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain(),
  metricsFactory: IMetricsFactory = new NullMetricsFactory(),
  checkPointInterval: FiniteDuration = 5.minutes,
  retryConfig: RetryConfig = RetryConfig(1.second, 1.second, 3),
  initialPositionInStream: InitialPositionInStream = InitialPositionInStream.LATEST
) {
  def withCommonCredentialsProvider(credentialsProvider: AWSCredentialsProvider): KinesisStreamConsumerConfig[T] =
    this.copy(
      kinesisCredentialsProvider = credentialsProvider,
      dynamoCredentialsProvider = credentialsProvider,
      cloudWatchCredentialsProvider = credentialsProvider
    )
}
