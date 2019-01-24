package com.intuit.aiml.infostore.processing.metadata.repository.atlas

import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.intuit.aiml.infostore.processing.metadata.repository.atlas.HTTP_METHOD.HTTP_METHOD

class RestClientSpec extends FlatSpec with BeforeAndAfterEach {

  private val port = 11999
  private val hostname = "localhost"
  val restClient = new RestClient

  private val wireMockServer = new WireMockServer(port)

  override def beforeEach {
    wireMockServer.start()
  }
  override def afterEach {
    wireMockServer.stop()
  }

  "Rest Client" should
    "send proper request" in {
    val path = "/some/api"
    val url: String = s"http://$hostname:$port/some/api"
    val method: HTTP_METHOD = HTTP_METHOD.GET
    val headers: Map[String, String] = Map.empty
    val queryParams: Seq[(String, String)] = Seq(("Partha", "Bangalore"), ("Rabi", "India"))
    val body: String = ""

    wireMockServer.stubFor(
      get(urlPathEqualTo(path))
        .willReturn(status(200))
    )

    // Send request by using your HTTP client
    val response = restClient.request(url, method, headers, queryParams, body)

    // Verify the request is valid
    wireMockServer.verify(
      getRequestedFor(urlPathEqualTo(path))
    )
  }

  "Rest Client" should
    "send post method related proper request" in {
    val path = "/posttest/api"
    val url: String = s"http://$hostname:$port/posttest/api"
    val method: HTTP_METHOD = HTTP_METHOD.POST
    val headers: Map[String, String] = Map.empty
    val queryParams: Seq[(String, String)] = Seq.empty
    val body: String = "This is POST Method"
    val responseBody = "Response for POST method"

    wireMockServer.stubFor(
      post(urlPathEqualTo(path))
        .willReturn(status(200)
          .withBody(responseBody)
          .withStatusMessage("OK"))
    )

    // Send request by using your HTTP client
    val response: (Int, String) = restClient.request(url, method, headers, queryParams, body)
    assert(response.equals(200,responseBody))

    // Verify the request is valid
    wireMockServer.verify(
      postRequestedFor(urlPathEqualTo(path))
    )
  }

  "Rest Client" should
    "send put method related proper request" in {
    val path = "/puttest/api"
    val url: String = s"http://$hostname:$port/puttest/api"
    val method: HTTP_METHOD = HTTP_METHOD.PUT
    val headers: Map[String, String] = Map(("Intuit", "BANGALORE"), ("INDIA", "KARNATAKA"))
    val queryParams: Seq[(String, String)] = Seq(("Bikash", "Bangalore"), ("Partha", "India"))
    val body: String = "This is PUT method"
    val responseBody = "Response for PUT method"

    wireMockServer.stubFor(
      put(urlPathEqualTo(path))
        .willReturn(status(404)
          .withHeader("Intuit", "BANGALORE")
            .withStatusMessage("Not Found")
             .withBody(responseBody))
    )

    // Send request by using your HTTP client
    val response = restClient.request(url, method, headers, queryParams, body)
    assert(response.equals(404,responseBody))

    // Verify the request is valid
    wireMockServer.verify(
      putRequestedFor(urlPathEqualTo(path))
         .withHeader("Intuit", containing("BANGALORE"))
         .withHeader("INDIA", containing("KARNATAKA"))
         .withQueryParam("Bikash",equalTo("Bangalore"))
         .withQueryParam("Partha",equalTo("India"))
    )
  }
}
