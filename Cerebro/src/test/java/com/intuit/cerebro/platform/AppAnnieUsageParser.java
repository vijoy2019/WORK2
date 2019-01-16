package com.intuit.cerebro.platform;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class AppAnnieUsageParser {

    private static final String USAGE_RESPONSE = "{\"code\": 200, \"unified_product_name\": \"Digit - Save Money Automatically\", \"vertical\": \"apps\", \"product_franchise_id\": null, \"product_category\": \"Finance\", \"parent_company_name\": \"Hello Digit\", \"company_id\": 1000200000120583, \"granularity\": \"daily\", \"device\": \"all\", \"product_code\": \"1011935076\", \"publisher_name\": \"Hello Digit, Inc\", \"market\": \"ios\", \"product_id\": 1011935076, \"end_date\": \"2017-06-25\", \"segment\": \"all_users\", \"list\": [{\"device\": \"ios\", \"date\": \"2017-06-25\", \"usage_penetration\": 0.02251, \"install_penetration\": 0.409182, \"open_rate\": 5.501122, \"retention_d1\": \"n/a\", \"retention_d7\": \"n/a\", \"retention_d30\": \"n/a\", \"active_users\": 24825, \"total_minutes\": \"n/a\", \"total_sessions\": \"n/a\", \"avg_sessions_per_user\": \"n/a\", \"avg_session_duration\": \"n/a\", \"avg_time_per_user\": \"n/a\", \"avg_active_days\": \"n/a\", \"percent_active_days\": \"n/a\", \"share_of_category_time\": \"n/a\", \"share_of_category_sessions\": \"n/a\", \"share_of_category_mb\": \"n/a\", \"mb_per_user\": \"n/a\", \"mb_per_session\": \"n/a\", \"percent_of_mb_wifi\": \"n/a\"}, {\"device\": \"iphone\", \"date\": \"2017-06-25\", \"usage_penetration\": 0.02251, \"install_penetration\": 0.409182, \"open_rate\": 5.501122, \"retention_d1\": \"n/a\", \"retention_d7\": \"n/a\", \"retention_d30\": \"n/a\", \"active_users\": 24825, \"total_minutes\": \"n/a\", \"total_sessions\": \"n/a\", \"avg_sessions_per_user\": \"n/a\", \"avg_session_duration\": \"n/a\", \"avg_time_per_user\": \"n/a\", \"avg_active_days\": \"n/a\", \"percent_active_days\": \"n/a\", \"share_of_category_time\": \"n/a\", \"share_of_category_sessions\": \"n/a\", \"share_of_category_mb\": \"n/a\", \"mb_per_user\": \"n/a\", \"mb_per_session\": \"n/a\", \"percent_of_mb_wifi\": \"n/a\"}], \"parent_company_id\": 1000200000120583, \"start_date\": \"2017-06-25\", \"unified_product_id\": 1000600000566369, \"product_franchise_name\": \"Digit - Save Money Automatically\", \"company_name\": \"Hello Digit\", \"country\": \"US\", \"publisher_id\": 1011935075, \"product_name\": \"Digit: Save Money Effortlessly\"}" ;

    public AppAnnieUsageParser() {

    }

    public static void main(String[] args) {
        AppAnnieUsageParser usageParser = new AppAnnieUsageParser();
        try {
            usageParser.parse();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void parse() throws Exception {
        System.out.println(USAGE_RESPONSE);
        JsonObject jsonObject = new JsonParser().parse(USAGE_RESPONSE).getAsJsonObject();
        JsonArray arr = jsonObject.getAsJsonArray("list");
        for (int i = 0; i < arr.size(); i++) {
            JsonElement jsonEle = arr.get(i);
            String active_users = jsonEle.getAsJsonObject().get("active_users").getAsString();
            System.out.println("active_users " + active_users);
            String avg_sessions_per_user = jsonEle.getAsJsonObject().get("avg_sessions_per_user").getAsString();
            System.out.println("avg_sessions_per_user " + avg_sessions_per_user);
            String avg_time_per_user = jsonEle.getAsJsonObject().get("avg_time_per_user").getAsString();
            System.out.println("avg_time_per_user " + avg_time_per_user);
            String usage_penetration = jsonEle.getAsJsonObject().get("usage_penetration").getAsString();
            System.out.println("usage_penetration " + usage_penetration);
            String install_penetration = jsonEle.getAsJsonObject().get("install_penetration").getAsString();
            System.out.println("install_penetration " + install_penetration);
        }
    }

}
