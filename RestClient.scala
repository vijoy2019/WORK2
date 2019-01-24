package com.intuit.aiml.infostore.processing.metadata.repository.atlas

import java.io.InputStream
import java.net.{HttpURLConnection, URL, URLEncoder}

import org.slf4j.{Logger, LoggerFactory}
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import com.intuit.aiml.infostore.processing.metadata.repository.atlas.HTTP_METHOD.HTTP_METHOD

/**
  * Provides basic HTTP Method implementation to make REST calls
  * Uses native Http support in JVM instead of external dependencies
  */
class RestClient {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val CONNECTION_TIMEOUT = 10000
  val CONNECTION_READ_TIMEOUT = 30000

  val get: HttpURLConnection => Unit = (connection: HttpURLConnection) => {
    connection.connect()
  }

  val post: (HttpURLConnection, String) => Unit = (connection: HttpURLConnection, body: String) => {
    connection.setDoOutput(true)
    connection.connect()
    connection.getOutputStream.write(body.getBytes())
  }

  val getResponse: (HttpURLConnection, InputStream) => (Int, String) =
    (connection: HttpURLConnection, inputStream: InputStream) => {
      val responseCode = connection.getResponseCode
      val content = scala.io.Source.fromInputStream(inputStream).mkString
      if (inputStream != null) inputStream.close()
      (responseCode, content)
    }

  private def closeStreams(connection: HttpURLConnection): Unit = {
    try {
      connection.getInputStream.close()
    } catch {
      case e: Exception => // ignore
    }
    try {
      connection.getErrorStream.close()
    } catch {
      case e: Exception => // ignore
    }
  }

  def buildURL(baseURL: String, queryParams: Seq[(String, String)]): String = {
    val encodedParams = if (queryParams.isEmpty) "" else {
      queryParams.map(p => URLEncoder.encode(p._1, "utf-8") + "=" + URLEncoder.encode(p._2, "utf-8")).mkString("&")
    }
    baseURL + (if (baseURL.contains("?")) {
      "&" + encodedParams
    }
    else {
      "?" + encodedParams
    })
  }

  def request(
    url: String,
    method: HTTP_METHOD,
    headers: Map[String, String],
    queryParams: Seq[(String, String)],
    body: String = ""
  ): (Int, String) = {
    val connection = new URL(buildURL(url, queryParams)).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(CONNECTION_TIMEOUT)
    connection.setReadTimeout(CONNECTION_READ_TIMEOUT)
    connection.setRequestMethod(method.toString)
    connection.setInstanceFollowRedirects(true)

    // Set Headers
    for ((prop, value) <- headers) {
      connection.setRequestProperty(prop, value)
    }

    // Make required connection
    method match {
      case HTTP_METHOD.GET => get(connection)
      case HTTP_METHOD.POST | HTTP_METHOD.PUT => post(connection, body)
      case _ => throw new NotImplementedException()
    }

    try {
      getResponse(connection, connection.getInputStream)
    } catch {
      case e: java.io.IOException =>
        getResponse(connection, connection.getErrorStream)
    } finally {
      closeStreams(connection)
    }
  }
}

object HTTP_METHOD extends Enumeration {
  type HTTP_METHOD = Value
  val GET, POST, PUT, DELETE = Value
}
