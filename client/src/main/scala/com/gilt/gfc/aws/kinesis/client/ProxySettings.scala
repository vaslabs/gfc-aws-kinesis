package com.gilt.gfc.aws.kinesis.client

import com.amazonaws.ClientConfiguration

/**
  * Created by nicolaouv on 08/03/17.
  */
trait ProxySettings {
  val host: String
  val port: Int
  val username: Option[String] = None
  val password: Option[String] = None
  private[client] def configureClientConfiguration(conf: ClientConfiguration): Unit = {
    conf.setProxyHost(this.host)
    conf.setProxyPort(this.port)
    this.username.foreach(conf.setProxyUsername(_))
    this.password.foreach(conf.setProxyPassword(_))
  }
}
