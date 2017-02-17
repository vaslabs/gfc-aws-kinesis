package com.gilt.gfc.aws.kinesis.client

import java.util.UUID

import com.amazonaws.auth.{AWSCredentialsProvider, AWSCredentialsProviderChain, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration


/** Configures KCL
  *
  * http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-record-processor-implementation-app-java.html
  * https://github.com/aws/aws-sdk-java/blob/master/src/samples/AmazonKinesisApplication/SampleKinesisApplication.java
  *
  * https://github.com/awslabs/amazon-kinesis-client
  */
object KCLConfiguration {

  val HostName = {
    import scala.sys.process._
    "hostname".!!.trim()
  }

  assert( HostName != null   , "Couldn't determine hostname, got null" )
  assert( ! HostName.isEmpty , "Couldn't determine hostname, got empty string" )

  /** Provides some initial config, can be further customized.
    * Mainly a point of reference for imports and doc links.
    *
    * @param applicationName name of the consumer
    *
    * @param streamName kinesis stream name
    */
  def apply( applicationName: String
           , streamName: String
           , kinesisCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain()
           , dynamoCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain()
           , cloudWatchCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain()
           , regionName: Option[String] = None
           ): KinesisClientLibConfiguration = {

    new KinesisClientLibConfiguration(
      // We want same app to process multiple versions of stream,
      // this name-spaces them to avoid name clash in dynamodb.
      s"${applicationName}.${streamName}"
    , streamName
    , kinesisCredentialsProvider
    , dynamoCredentialsProvider
    , cloudWatchCredentialsProvider
    , s"${HostName}:${UUID.randomUUID()}"
    ).withRegionName(regionName.orNull)
  }
}
