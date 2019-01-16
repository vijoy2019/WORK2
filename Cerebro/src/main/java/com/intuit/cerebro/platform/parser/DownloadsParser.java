package com.intuit.cerebro.platform.parser;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.intuit.cerebro.platform.schema.DownloadSchema;

import java.util.Map;

public class DownloadsParser implements RequestHandler<DynamodbEvent, String>, ResponseFormatter {
    private static final String STREAM_NAME = System.getenv("STREAM_NAME");
    //private static final String STREAM_NAME = "downloadsappannie";

    public String handleRequest(DynamodbEvent ddbEvent, Context context) {
        AppAnnieParser parser = new AppAnnieParser(this, STREAM_NAME);
        return parser.parseResponse(ddbEvent, context);
    }

    public String format(Map<String, AttributeValue> values) throws JsonProcessingException {
        DownloadSchema downloadSchema = new DownloadSchema();
        downloadSchema.setApp(values.get("Company").getS());
        downloadSchema.setMarket(values.get("Market").getS());
        downloadSchema.setRundate(values.get("StartDate").getS());
        downloadSchema.setProductId(values.get("ProductId").getS());
        String payLoad = values.get("Payload").getS();
        String estimate = estimate(payLoad);
        downloadSchema.setDownloads(estimate);
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(downloadSchema);
        return json;
    }

    private String estimate(String payLoad) {
        double iEstimate = 0;
        JsonObject jsonObject = new JsonParser().parse(payLoad).getAsJsonObject();
        JsonArray arr = jsonObject.getAsJsonArray("list");
        for (int i = 0; i < arr.size(); i++) {
            JsonElement estimate = arr.get(i).getAsJsonObject().get("estimate");
            if (estimate != null && !estimate.isJsonNull()) {
                String sEstimate = estimate.getAsString();
                try {
                    double transEstimate = Double.parseDouble(sEstimate);
                    iEstimate = iEstimate + transEstimate;
                } catch (NumberFormatException nex) {
                    System.out.println("Failed to parse " + estimate + " with error " + nex.getMessage());
                    //Ignore App Annie stores N/A for some of response
                }
            }
        }
        return String.valueOf(iEstimate);
    }

}
