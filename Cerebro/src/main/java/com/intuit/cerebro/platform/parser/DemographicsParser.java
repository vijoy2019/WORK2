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
import com.intuit.cerebro.platform.schema.Demographic;
import com.intuit.cerebro.platform.schema.DemographicSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DemographicsParser implements RequestHandler<DynamodbEvent, String>, ResponseFormatter {
    private static final String STREAM_NAME = System.getenv("STREAM_NAME");
    //private static final String STREAM_NAME = "demographicappappannie";

    public String handleRequest(DynamodbEvent ddbEvent, Context context) {
        AppAnnieParser parser = new AppAnnieParser(this, STREAM_NAME);
        return parser.parseResponse(ddbEvent, context);
    }

    public String format(Map<String, AttributeValue> values) throws JsonProcessingException {
        DemographicSchema demographicSchema = new DemographicSchema();
        demographicSchema.setApp(values.get("Company").getS());
        demographicSchema.setMarket(values.get("Market").getS());
        demographicSchema.setRundate(values.get("StartDate").getS());
        demographicSchema.setProductId(values.get("ProductId").getS());
        String payLoad = values.get("Payload").getS();

        JsonObject jsonObject = new JsonParser().parse(payLoad).getAsJsonObject();
        JsonArray arr = jsonObject.getAsJsonArray("list");
        List<Demographic> demographicList = new ArrayList<>();
        for (int i = 0; i < arr.size(); i++) {
            Demographic demographic = new Demographic();
            JsonObject js1 = arr.get(i).getAsJsonObject();
            JsonElement jEle = js1.get("age_index_25_44");
            if (!jEle.isJsonNull()) {
                demographic.setAge_index_25_44(jEle.getAsString());
            }
            jEle = js1.get("age_percent_16_24");
            if (!jEle.isJsonNull()) {
                demographic.setAge_percent_16_24(jEle.getAsString());
            }
            jEle = js1.get("end_date");
            if (!jEle.isJsonNull()) {
                demographic.setEnd_date(jEle.getAsString());
            }
            jEle = js1.get("gender_percent_female");
            if (!jEle.isJsonNull()) {
                demographic.setGender_percent_female(jEle.getAsString());
            }
            jEle = js1.get("gender_index_female");
            if (!jEle.isJsonNull()) {
                demographic.setGender_index_female(jEle.getAsString());
            }
            jEle = js1.get("age_index_16_24");
            if (!jEle.isJsonNull()) {
                demographic.setAge_index_16_24(jEle.getAsString());
            }
            jEle = js1.get("gender_percent_male");
            if (!jEle.isJsonNull()) {
                demographic.setGender_percent_male(jEle.getAsString());
            }
            jEle = js1.get("gender_index_male");
            if (!jEle.isJsonNull()) {
                demographic.setGender_index_male(jEle.getAsString());
            }
            jEle = js1.get("age_index_45_plus");
            if (!jEle.isJsonNull()) {
                demographic.setAge_index_45_plus(jEle.getAsString());
            }
            jEle = js1.get("age_percent_45_plus");
            if (!jEle.isJsonNull()) {
                demographic.setAge_percent_45_plus(jEle.getAsString());
            }
            jEle = js1.get("start_date");
            if (!jEle.isJsonNull()) {
                demographic.setStart_date(jEle.getAsString());
            }
            jEle = js1.get("age_percent_25_44");
            if (!jEle.isJsonNull()) {
                demographic.setAge_percent_25_44(jEle.getAsString());
            }
            demographicList.add(demographic);
        }
        Demographic[] demographics = demographicList.toArray(new Demographic[demographicList.size()]);
        demographicSchema.setDemographics(demographics);
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(demographicSchema);
        return json;
    }

}
