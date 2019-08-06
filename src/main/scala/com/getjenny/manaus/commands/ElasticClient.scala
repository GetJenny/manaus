package com.getjenny.manaus.commands

/**
  * Created by Angelo Leto <angelo@getjenny.com> on 01/07/16.
  */

import java.net.InetAddress

import javax.net.ssl._
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.http.message.BasicHeader
import org.apache.http.{Header, HttpHost}
import org.elasticsearch.client.{RestClient, RestClientBuilder, RestHighLevelClient}
import scalaz.Scalaz._

import scala.collection.immutable.{List, Map}

trait ElasticClient {
  val clusterName: String
  val ignoreClusterName: Boolean
  val elasticsearchAuthentication: String

  val indexName: String
  val indexSuffix: String

  val hostProto: String
  val hostMap: Map[String, Int]

  val keystorePath: String
  val keystorePassword: String
  val certFormat: String

  val sslContext: SSLContext = certFormat match {
    case "jks" =>
      val path = keystorePath
      val password = keystorePassword
      SslContext.jks(path, password)
    case "pkcs12" | _ =>
      val path = keystorePath
      val password = keystorePassword
      SslContext.pkcs12(path, password)
  }
  val disableHostValidation: Boolean

  val inetAddresses: List[HttpHost] =
    hostMap.map { case (k, v) => new HttpHost(InetAddress.getByName(k), v, hostProto) }.toList


  object AllHostsValid extends HostnameVerifier {
    def verify(hostname: String, session: SSLSession) = true
  }

  object HttpClientConfigCallback extends RestClientBuilder.HttpClientConfigCallback {
    override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
      val httpBuilder = httpClientBuilder.setSSLContext(sslContext)
      if (disableHostValidation)
        httpBuilder.setSSLHostnameVerifier(AllHostsValid)
      httpBuilder
    }
  }

  def buildClient(hostProto: String): RestClientBuilder = {
    val headerValues: Array[(String, String)] = Array(("Authorization", "Basic " + elasticsearchAuthentication))
    val defaultHeaders: Array[Header] = headerValues.map { case (key, value) =>
      new BasicHeader(key, value)
    }
    if (hostProto === "http") {
      RestClient.builder(inetAddresses: _*)
    } else {
      RestClient.builder(inetAddresses: _*)
        .setHttpClientConfigCallback(HttpClientConfigCallback)
    }.setDefaultHeaders(defaultHeaders)
  }

  private[this] var esHttpClient: RestHighLevelClient = openHttp()

  def openHttp(): RestHighLevelClient = {
    val client: RestHighLevelClient = new RestHighLevelClient(
      buildClient(hostProto)
    )
    client
  }

  def httpClient: RestHighLevelClient = {
    this.esHttpClient
  }

  def closeHttp(client: RestHighLevelClient): Unit = {
    client.close()
  }
}
