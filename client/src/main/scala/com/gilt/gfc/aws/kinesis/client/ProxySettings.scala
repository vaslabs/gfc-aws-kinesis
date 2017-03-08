package com.gilt.gfc.aws.kinesis.client

import com.amazonaws.ClientConfiguration

/**
  * Created by nicolaouv on 08/03/17.
  */
case class ProxySettings(host: String, port: Int, username: Option[String] = None, password: Option[String] = None) {
  private[client] def configureClientConfiguration(conf: ClientConfiguration): Unit = {
    conf.setProxyHost(this.host)
    conf.setProxyPort(this.port)
    this.username.foreach(conf.setProxyUsername(_))
    this.password.foreach(conf.setProxyPassword(_))
  }
}
