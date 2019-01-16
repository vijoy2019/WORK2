package com.intuit.cerebro.platform.collector.monthly;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.intuit.cerebro.platform.GatewayResponse;
import com.intuit.cerebro.platform.collector.DataCollectionWorker;
import com.intuit.cerebro.platform.collector.DoneConfig;
import com.intuit.cerebro.platform.dynamodb.DynamoDBHandler;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;


public class MonthlyDataCollector implements RequestHandler<Object, Object> {

    public Object handleRequest(final Object input, final Context context) {
        startDataCollection();
        return getPageContents();
    }

    private void startDataCollection() {
        DataCollectionWorker impl = new DataCollectionWorker();
        try {
            DynamoDBHandler.getInstance().enableMonthMode();
            impl.doDataCollection(DoneConfig.MODE.MONTH);
        } catch (ParseException | IOException e) {
            System.out.println(e.getMessage());
        }
    }

    private GatewayResponse getPageContents() {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("X-Custom-Header", "application/json");
        String pageContents = "Executed at " + System.currentTimeMillis();
        return new GatewayResponse(pageContents, headers, 200);
    }

}
