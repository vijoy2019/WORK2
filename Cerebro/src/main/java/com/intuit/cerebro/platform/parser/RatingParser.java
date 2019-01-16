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
import com.intuit.cerebro.platform.schema.AppRatingSchema;

import java.util.Map;

public class RatingParser implements RequestHandler<DynamodbEvent, String>, ResponseFormatter {
    private static final String STREAM_NAME = System.getenv("STREAM_NAME");
    //private static final String STREAM_NAME = "ratingappannie";

    public String handleRequest(DynamodbEvent ddbEvent, Context context) {
        AppAnnieParser parser = new AppAnnieParser(this, STREAM_NAME);
        return parser.parseResponse(ddbEvent, context);
    }

    public String format(Map<String, AttributeValue> values) throws JsonProcessingException {
        AppRatingSchema appSchema = new AppRatingSchema();
        appSchema.setApp(values.get("Company").getS());
        appSchema.setMarket(values.get("Market").getS());
        appSchema.setRundate(values.get("StartDate").getS());
        appSchema.setProductId(values.get("ProductId").getS());

        String payLoad = values.get("Payload").getS();
        JsonObject jsonObject = new JsonParser().parse(payLoad).getAsJsonObject();
        JsonArray arr = jsonObject.getAsJsonArray("ratings");
        JsonObject jsonEle = arr.get(0).getAsJsonObject();

        JsonObject curRating = jsonEle.getAsJsonObject("current_ratings");
        JsonElement jElement = curRating.get("average");
        if(jElement != null && !jElement.isJsonNull()) {
            appSchema.setCu_average(jElement.getAsString());
        }
        jElement = curRating.get("rating_count");
        if(jElement != null && !jElement.isJsonNull()) {
            appSchema.setCu_rating_count(jElement.getAsString());
        }
        jElement = curRating.get("star_5_count");
        if(jElement != null && !jElement.isJsonNull()) {
            appSchema.setCu_star_5_count(jElement.getAsString());
        }
        jElement = curRating.get("star_4_count");
        if(jElement != null && !jElement.isJsonNull()) {
            appSchema.setCu_star_4_count(jElement.getAsString());
        }
        jElement = curRating.get("star_3_count");
        if(jElement != null && !jElement.isJsonNull()) {
            appSchema.setCu_star_3_count(jElement.getAsString());
        }
        jElement = curRating.get("star_2_count");
        if(jElement != null && !jElement.isJsonNull()) {
            appSchema.setCu_star_2_count(jElement.getAsString());
        }
        jElement = curRating.get("star_1_count");
        if(jElement != null && !jElement.isJsonNull()) {
            appSchema.setCu_star_1_count(jElement.getAsString());
        }

        JsonObject allRating = jsonEle.getAsJsonObject("all_ratings");
        jElement = allRating.get("average");
        if(jElement != null && !jElement.isJsonNull()) {
            appSchema.setAll_average(jElement.getAsString());
        }
        jElement = allRating.get("rating_count");
        if(jElement != null && !jElement.isJsonNull()) {
            appSchema.setAll_rating_count(jElement.getAsString());
        }
        jElement = allRating.get("star_5_count");
        if(jElement != null && !jElement.isJsonNull()) {
            appSchema.setAll_star_5_count(jElement.getAsString());
        }
        jElement = allRating.get("star_4_count");
        if(jElement != null && !jElement.isJsonNull()) {
            appSchema.setAll_star_4_count(jElement.getAsString());
        }
        jElement = allRating.get("star_3_count");
        if(jElement != null && !jElement.isJsonNull()) {
            appSchema.setAll_star_3_count(jElement.getAsString());
        }
        jElement = allRating.get("star_2_count");
        if(jElement != null && !jElement.isJsonNull()) {
            appSchema.setAll_star_2_count(jElement.getAsString());
        }
        jElement = allRating.get("star_1_count");
        if(jElement != null && !jElement.isJsonNull()) {
            appSchema.setAll_star_1_count(jElement.getAsString());
        }

        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(appSchema);
        return json;
    }

}
