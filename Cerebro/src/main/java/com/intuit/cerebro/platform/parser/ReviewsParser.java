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
import com.intuit.cerebro.platform.schema.Review;
import com.intuit.cerebro.platform.schema.ReviewSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ReviewsParser implements RequestHandler<DynamodbEvent, String>, ResponseFormatter {
    private static final String STREAM_NAME = System.getenv("STREAM_NAME");
    //private static final String STREAM_NAME = "reviewappannie";

    public String handleRequest(DynamodbEvent ddbEvent, Context context) {
        AppAnnieParser parser = new AppAnnieParser(this, STREAM_NAME);
        return parser.parseResponse(ddbEvent, context);
    }

    public String format(Map<String, AttributeValue> values) throws JsonProcessingException {
        ReviewSchema reviewSchema = new ReviewSchema();
        reviewSchema.setApp(values.get("Company").getS());
        reviewSchema.setMarket(values.get("Market").getS());
        reviewSchema.setRundate(values.get("StartDate").getS());
        reviewSchema.setProductId(values.get("ProductId").getS());
        String payLoad = values.get("Payload").getS();
        JsonObject jsonObject = new JsonParser().parse(payLoad).getAsJsonObject();
        JsonArray arr = jsonObject.getAsJsonArray("reviews");
        List<Review> reviewList = new ArrayList<>();
        for (int i = 0; i < arr.size(); i++) {
            Review review = new Review();
            JsonObject js1 = arr.get(i).getAsJsonObject();
            JsonElement rating = js1.get("rating");
            if(!rating.isJsonNull()) {
                review.setRating(rating.getAsString());
            }
            JsonElement text = js1.get("text");
            if(!text.isJsonNull()) {
                review.setText(text.getAsString());
            }
            JsonElement language = js1.get("language");
            if(!language.isJsonNull()) {
                review.setLanguage(language.getAsString());
            }
            JsonElement title = js1.get("title");
            if(!title.isJsonNull()) {
                review.setTitle(title.getAsString());
            }
            JsonElement reviewDate = js1.get("date");
            if(!reviewDate.isJsonNull()) {
                review.setReviewDate(reviewDate.getAsString());
            }
            JsonElement country = js1.get("country");
            if(!country.isJsonNull()) {
                review.setCountry(country.getAsString());
            }
            reviewList.add(review);
        }
        Review[] reviews = reviewList.toArray(new Review[reviewList.size()]);
        reviewSchema.setReview(reviews);
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(reviewSchema);
        return json;
    }

}
