package com.intuit.cerebro.platform.parser;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.util.StringUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.intuit.cerebro.platform.schema.CrossApp;
import com.intuit.cerebro.platform.schema.CrossAppSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CrossAppParser implements RequestHandler<DynamodbEvent, String>, ResponseFormatter {
    private static final String STREAM_NAME = System.getenv("STREAM_NAME");

    public String handleRequest(DynamodbEvent ddbEvent, Context context) {
        AppAnnieParser parser = new AppAnnieParser(this, STREAM_NAME);
        return parser.parseResponse(ddbEvent, context);
    }

    public String format(Map<String, AttributeValue> values) throws JsonProcessingException {
        CrossAppSchema crossAppSchema = new CrossAppSchema();
        crossAppSchema.setApp(values.get("Company").getS());
        crossAppSchema.setMarket(values.get("Market").getS());
        crossAppSchema.setRundate(values.get("StartDate").getS());
        crossAppSchema.setProductId(values.get("ProductId").getS());
        String payLoad = values.get("Payload").getS();
        parseResponse(crossAppSchema, payLoad);
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(crossAppSchema);
        return json;
    }

    private void parseResponse(CrossAppSchema crossAppSchema, String payLoad) {
        JsonObject jsonObject = new JsonParser().parse(payLoad).getAsJsonObject();
        JsonElement category = jsonObject.get("category");
        crossAppSchema.setCategory("all");
        if (!category.isJsonNull()) {
            String sCategory = category.getAsString();
            if (!StringUtils.isNullOrEmpty(sCategory)) {
                if (sCategory.contains("FINANCE") || sCategory.contains("finance")) {
                    crossAppSchema.setCategory("finance");
                } else if (sCategory.contains("BUSINESS") || sCategory.contains("business")) {
                    crossAppSchema.setCategory("business");
                }
            }
        }
        JsonArray arr = jsonObject.getAsJsonArray("list");
        List<CrossApp> crossAppList = new ArrayList<>();
        for (int i = 0; i < arr.size(); i++) {
            CrossApp crossApp = new CrossApp();
            JsonObject js1 = arr.get(i).getAsJsonObject();
            JsonElement cross_app_product_code = js1.get("cross_app_product_code");
            if (!cross_app_product_code.isJsonNull()) {
                crossApp.setCross_app_product_code(cross_app_product_code.getAsString());
            }
            JsonElement cross_app_parent_company_name = js1.get("cross_app_parent_company_name");
            if (!cross_app_parent_company_name.isJsonNull()) {
                crossApp.setCross_app_parent_company_name(cross_app_parent_company_name.getAsString());
            }
            JsonElement cross_app_unified_product_id = js1.get("cross_app_unified_product_id");
            if (!cross_app_unified_product_id.isJsonNull()) {
                crossApp.setCross_app_product_code(cross_app_unified_product_id.getAsString());
            }
            JsonElement rank = js1.get("rank");
            if (!rank.isJsonNull()) {
                crossApp.setRank(rank.getAsString());
            }
            JsonElement affinity = js1.get("affinity");
            if (!affinity.isJsonNull()) {
                crossApp.setAffinity(affinity.getAsString());
            }
            JsonElement cross_app_usage = js1.get("cross_app_usage");
            if (!cross_app_usage.isJsonNull()) {
                crossApp.setCross_app_usage(cross_app_usage.getAsString());
            }
            crossAppList.add(crossApp);
        }
        CrossApp[] crossApps = crossAppList.toArray(new CrossApp[crossAppList.size()]);
        crossAppSchema.setCrossApp(crossApps);
    }

}
