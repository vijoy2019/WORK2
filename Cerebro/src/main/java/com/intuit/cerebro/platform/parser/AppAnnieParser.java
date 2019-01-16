package com.intuit.cerebro.platform.parser;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.intuit.cerebro.platform.kinesis.KinesisHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AppAnnieParser {

    private ResponseFormatter responseFormatter;
    private String streamName;

    public AppAnnieParser(ResponseFormatter responseFormatter, String streamName) {
        this.responseFormatter = responseFormatter;
        this.streamName = streamName;
    }

    public String parseResponse(DynamodbEvent ddbEvent, Context context) {
        LambdaLogger logger = context.getLogger();
        List<String> recordList = new ArrayList<>();
        for (DynamodbEvent.DynamodbStreamRecord record : ddbEvent.getRecords()) {
            if (!record.getEventName().equals("INSERT")) {
                System.out.println("Not an insert evert so ignoring " + record.getEventName());
                return "";
            }
            try {
                Map<String, AttributeValue> values = record.getDynamodb().getNewImage();
                String json = responseFormatter.format(values);
                recordList.add(json);
                logger.log(json);
            } catch (Exception e) {
                logger.log("Error while persisting data to kenisis " + e.getMessage());
            }
        }
        if (!recordList.isEmpty()) {
            KinesisHandler.getInstance().putRecords(recordList, streamName);
        }
        return "Successfully processed " + ddbEvent.getRecords().size() + " records.";
    }

}
