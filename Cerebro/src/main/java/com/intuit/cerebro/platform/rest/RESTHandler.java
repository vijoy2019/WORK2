package com.intuit.cerebro.platform.rest;


import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.util.IOUtils;
import com.intuit.cerebro.platform.schema.RequestConfig;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;


public class RESTHandler {

    private final Client client;

    public RESTHandler() {
        client = ClientBuilder.newBuilder().newClient();
    }

    public String getResponse(RequestConfig restConfig) throws IOException {
        String baseRequest = restConfig.getBaseUrl();
        Map<String, AttributeValue> headers = restConfig.getHeaders();
        Map<String, AttributeValue> params = restConfig.getParams();
        WebTarget target = client.target(baseRequest);
        for (Map.Entry<String, AttributeValue> entry : params.entrySet()) {
            target = target.queryParam(entry.getKey(), entry.getValue().getS());
        }
        Invocation.Builder builder = target.request();
        for (Map.Entry<String, AttributeValue> entry : headers.entrySet()) {
            builder = builder.header(entry.getKey(), entry.getValue().getS());
        }
        Response response = builder.get();
        if(response.getStatus() == 200) {
            System.out.println(" [ "+restConfig.getTarget()+" ] [ " + restConfig.getStartDate()+ " ] [ Base Url "+restConfig.getBaseUrl() +" ]" );
            return IOUtils.toString((InputStream) response.getEntity());
        } else if(response.getStatus() == 403) {
            System.out.println(" [ "+restConfig.getTarget()+" ] [ " + restConfig.getStartDate()+ " ] [ Base Url "+restConfig.getBaseUrl() +" ]" );
            System.out.println("No response as: "+IOUtils.toString((InputStream) response.getEntity()));
            return "";
        }
        else {
            System.out.println("Failed: Calling Target [ "+restConfig.getTarget()+ " ] for url [ "+restConfig.getBaseUrl()+" ]");
            System.out.println("Failed: Response "+IOUtils.toString((InputStream) response.getEntity()));
            throw new IOException("");
        }
    }

}
