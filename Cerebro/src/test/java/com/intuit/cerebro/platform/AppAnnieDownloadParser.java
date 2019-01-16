package com.intuit.cerebro.platform;


import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class AppAnnieDownloadParser {

    private static final String DOWNLOAD_RESPONSE = "{\n" +
            "   \"feed\": \"paid_downloads\",\n" +
            "   \"code\": 200,\n" +
            "   \"product_id\": 553834731,\n" +
            "   \"vertical\": \"apps\",\n" +
            "   \"country\": \"US\",\n" +
            "   \"list\": [\n" +
            "       {\n" +
            "           \"device\": \"iphone\",\n" +
            "           \"estimate\": 2488,\n" +
            "           \"final\": true,\n" +
            "           \"end_date\": \"2016-09-01\",\n" +
            "           \"start_date\": \"2016-09-01\"    \n" +
            "       },\n" +
            "       {\n" +
            "           \"device\": \"ipad\",\n" +
            "           \"estimate\": 1567,\n" +
            "           \"final\": true,\n" +
            "           \"end_date\": \"2016-09-01\",\n" +
            "           \"start_date\": \"2016-09-01\"  \n" +
            "       },\n" +
            "       {\n" +
            "           \"device\": \"iphone\",\n" +
            "           \"estimate\": 2899,\n" +
            "           \"final\": true,\n" +
            "           \"end_date\": \"2016-09-02\",\n" +
            "           \"start_date\": \"2016-09-02\"\n" +
            "       },\n" +
            "       {\n" +
            "           \"device\": \"iphone\",\n" +
            "           \"estimate\": 1876,\n" +
            "           \"final\": true,\n" +
            "           \"end_date\": \"2016-09-02\",\n" +
            "           \"start_date\": \"2016-09-02\"\n" +
            "       }\n" +
            "   ],\n" +
            "   \"currency\": \"\",\n" +
            "   \"granularity\": \"daily\",\n" +
            "   \"market\": \"ios\"\n" +
            "}";


    public AppAnnieDownloadParser() {

    }

    public void parse() throws Exception {
        System.out.println(DOWNLOAD_RESPONSE);
        JsonObject jsonObject = new JsonParser().parse(DOWNLOAD_RESPONSE).getAsJsonObject();
        JsonArray arr = jsonObject.getAsJsonArray("list");
        for (int i = 0; i < arr.size(); i++) {
            JsonElement estimate = arr.get(i).getAsJsonObject().get("estimate");
            if(estimate != null) {
                String post_id = estimate.getAsString();
                System.out.println(post_id);
            }
        }
    }

    public static void main(String[] args) {
        AppAnnieDownloadParser parser = new AppAnnieDownloadParser();
        try {
            parser.parse();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
