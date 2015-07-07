package com.gilt.gfc.aws.kinesis.client

import java.util.UUID

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration


/** Configures KCL to connect to mobile tapstream.
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


  /** This needs to be called to force Java to refresh cached DNS names of AWS service end-points. */
  def setJavaDnsCacheTtl(ttl: Int) {
    require(ttl > 0, s"TTL must be >0, got: ${ttl}")
    java.security.Security.setProperty("networkaddress.cache.ttl", ttl.toString)
  }


  // Se it on object init, kind of a hack but seems important enough that we shouldn't forget.
  // Can be reset to another value if required.
  setJavaDnsCacheTtl(60)


  /** Provides some initial config for mobile tapstreams, can be further customized.
    * Mainly a point of reference for imports and doc links.
    *
    * @param applicationName name of the tapstream consumer
    *
    * @param streamName kinesis stream name. Mobile Kinesis tapstreams follow avdl namespaces in
    *                   https://github.com/gilt/events-mobile-tapstream
    *                   E.g. com.gilt.mobile.tapstream.v3
    */
  def apply( applicationName: String
           , streamName: String
           ): KinesisClientLibConfiguration = {

    new KinesisClientLibConfiguration(
      // We want same app to process multiple versions of tapstream,
      // this name-spaces them to avoid name clash in dynamodb.
      s"${applicationName}.${streamName}"
    , streamName
    , new DefaultAWSCredentialsProviderChain()
    , s"${HostName}:${UUID.randomUUID()}"
    )
  }
}

