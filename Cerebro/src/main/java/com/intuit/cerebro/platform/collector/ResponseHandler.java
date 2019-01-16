package com.intuit.cerebro.platform.collector;

import com.intuit.cerebro.platform.dynamodb.DynamoDBHandler;
import com.intuit.cerebro.platform.schema.RequestConfig;

public class ResponseHandler {

    public ResponseHandler () {

    }

    public void handleResponse(String response, RequestConfig restConfig) {
        DynamoDBHandler.getInstance().saveResponse(response, restConfig);
    }

}
