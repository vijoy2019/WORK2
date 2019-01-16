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
import com.intuit.cerebro.platform.schema.UsageSchema;

import java.util.Map;

public class UsageParser implements RequestHandler<DynamodbEvent, String>, ResponseFormatter {
    private static final String STREAM_NAME = System.getenv("STREAM_NAME");
    //private static final String STREAM_NAME = "usageappannie";

    public String handleRequest(DynamodbEvent ddbEvent, Context context) {
        AppAnnieParser parser = new AppAnnieParser(this, STREAM_NAME);
        return parser.parseResponse(ddbEvent, context);
    }

    public String format(Map<String, AttributeValue> values) throws JsonProcessingException {
        UsageSchema schema = new UsageSchema();
        schema.setApp(values.get("Company").getS());
        schema.setMarket(values.get("Market").getS());
        schema.setRundate(values.get("StartDate").getS());
        schema.setProductId(values.get("ProductId").getS());
        String payLoad = values.get("Payload").getS();
        parseResponse(schema, payLoad);
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(schema);
        return json;
    }

    private void parseResponse(UsageSchema schema, String payLoad) {
        JsonObject jsonObject = new JsonParser().parse(payLoad).getAsJsonObject();
        JsonArray arr = jsonObject.getAsJsonArray("list");
        double active_users = 0;
        double avg_sessions_per_user = 0;
        double avg_time_per_user = 0;
        double usage_penetration = 0;
        double install_penetration = 0;
        double install_base = 0;
        for (int i = 0; i < arr.size(); i++) {
            JsonElement jsonEle = arr.get(i);
            active_users = active_users + getValue(jsonEle, "active_users");
            avg_sessions_per_user = avg_sessions_per_user + getValue(jsonEle, "avg_sessions_per_user");
            avg_time_per_user = avg_time_per_user + getValue(jsonEle, "avg_time_per_user");
            usage_penetration = usage_penetration + getValue(jsonEle, "usage_penetration");
            install_penetration = install_penetration + getValue(jsonEle, "install_penetration");
        }
        if (usage_penetration != 0 && install_penetration != 0) {
            install_base = (active_users / usage_penetration) * install_penetration;
        }
        schema.setActive_users(String.valueOf(active_users));
        schema.setAvg_sessions_per_user(String.valueOf(avg_sessions_per_user));
        schema.setAvg_time_per_user(String.valueOf(avg_time_per_user));
        schema.setUsage_penetration(String.valueOf(usage_penetration));
        schema.setInstall_penetration(String.valueOf(install_penetration));
        schema.setInstall_base(String.valueOf(install_base));
    }

    private double getValue(JsonElement jsonEle, String elementName) {
        double dVal = 0;
        JsonElement jsonElement = jsonEle.getAsJsonObject().get(elementName);
        if (jsonElement != null && !jsonElement.isJsonNull()) {
            String sVal = jsonElement.getAsString();
            try {
                dVal = Double.parseDouble(sVal);
            } catch (NumberFormatException nex) {
                System.out.println("Failed to parse " + sVal + " with error " + nex.getMessage());
                //Ignore App Annie stores N/A for some of response
            }
        }
        return dVal;
    }

}
