package com.intuit.cerebro.platform;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class PopulateConfigTable {

    private static final String MONTHLY_CONFIG_TABLE = "Config_APIConfig_Monthly";
    private static final String DAILY_CONFIG_TABLE = "Config_APIConfig";

    private static String CONFIG_TABLE = DAILY_CONFIG_TABLE;

    private static void setMonthlyTable() {
        CONFIG_TABLE = MONTHLY_CONFIG_TABLE;
    }

    public static void populateiOSDownloadConfig(String url, String productid, String id, String targetTable, String companyName, String market, boolean isMonthly) throws Exception {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2)
                .withCredentials(new ProfileCredentialsProvider("cerebro")).build();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(CONFIG_TABLE);

        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer 978a43de14cf3d36a78458152b6b615e1688b6c1");

        Map<String, String> params = new HashMap<>();
        params.put("countries", "US");
        params.put("feeds", "downloads");
        params.put("device", "all");
        if (isMonthly) {
            params.put("granularity", "monthly");
        } else {
            params.put("granularity", "daily");
        }
        params.put("start_date", "START_DATE_yyyy-MM-dd");
        params.put("end_date", "END_DATE_yyyy-MM-dd");

        Item item = new Item();
        item.withPrimaryKey("Id", id);
        item.withString("BaseURL", url);
        item.withMap("Headers", headers);
        item.withMap("QueryParams", params);
        item.withString("CompanyName", companyName);
        item.withString("Market", market);
        item.withString("Target", targetTable);
        item.withString("ProductId", productid);
        try {
            PutItemOutcome outcome = table.putItem(item);
            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public static void populateAndriodDownloadConfig(String url, String productid, String id, String targetTable, String companyName, String market, boolean isMonthly) throws Exception {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2)
                .withCredentials(new ProfileCredentialsProvider("cerebro")).build();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(CONFIG_TABLE);

        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer 978a43de14cf3d36a78458152b6b615e1688b6c1");

        Map<String, String> params = new HashMap<>();
        params.put("countries", "US");
        params.put("feeds", "downloads");
        params.put("device", "android");
        if (isMonthly) {
            params.put("granularity", "monthly");
        } else {
            params.put("granularity", "daily");
        }
        params.put("start_date", "START_DATE_yyyy-MM-dd");
        params.put("end_date", "END_DATE_yyyy-MM-dd");

        Item item = new Item();
        item.withPrimaryKey("Id", id);
        item.withString("BaseURL", url);
        item.withMap("Headers", headers);
        item.withMap("QueryParams", params);
        item.withString("CompanyName", companyName);
        item.withString("Market", market);
        item.withString("Target", targetTable);
        item.withString("ProductId", productid);
        try {
            PutItemOutcome outcome = table.putItem(item);
            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public static void populateUsageConfig(String url, String productid, String id, String targetTable, String companyName, String market, boolean isMonthly) throws Exception {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2)
                .withCredentials(new ProfileCredentialsProvider("cerebro")).build();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(CONFIG_TABLE);

        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer 978a43de14cf3d36a78458152b6b615e1688b6c1");

        Map<String, String> params = new HashMap<>();
        params.put("countries", "US");
        params.put("device", "all");
        params.put("start_date", "START_DATE_yyyy-MM-dd");
        params.put("end_date", "END_DATE_yyyy-MM-dd");
        if (isMonthly) {
            params.put("granularity", "monthly");
        } else {
            params.put("granularity", "daily");
        }
        Item item = new Item();
        item.withPrimaryKey("Id", id);
        item.withString("BaseURL", url);
        item.withMap("Headers", headers);
        item.withMap("QueryParams", params);
        item.withString("CompanyName", companyName);
        item.withString("Market", market);
        item.withString("Target", targetTable);
        item.withString("ProductId", productid);
        try {
            PutItemOutcome outcome = table.putItem(item);
            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public static void populateCrossAppConfig(String url, String productid, String id, String targetTable, String companyName, String market, String category) throws Exception {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2)
                .withCredentials(new ProfileCredentialsProvider("cerebro")).build();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(CONFIG_TABLE);

        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer 978a43de14cf3d36a78458152b6b615e1688b6c1");

        Map<String, String> params = new HashMap<>();
        params.put("countries", "US");
        if (category != null) {
            params.put("categories", category);
        }

        params.put("start_date", "START_DATE_yyyy-MM-dd");
        params.put("end_date", "END_DATE_yyyy-MM-dd");

        Item item = new Item();
        item.withPrimaryKey("Id", id);
        item.withString("BaseURL", url);
        item.withMap("Headers", headers);
        item.withMap("QueryParams", params);
        item.withString("CompanyName", companyName);
        item.withString("Market", market);
        item.withString("Target", targetTable);
        item.withString("ProductId", productid);
        try {
            PutItemOutcome outcome = table.putItem(item);
            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public static void populateReviewsAPI(String url, String productid, String id, String targetTable, String companyName, String market) throws Exception {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2)
                .withCredentials(new ProfileCredentialsProvider("cerebro")).build();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(CONFIG_TABLE);

        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer 978a43de14cf3d36a78458152b6b615e1688b6c1");

        Map<String, String> params = new HashMap<>();
        if(market.equals("iOS")) {
            params.put("countries", "US");
        }
        params.put("start_date", "START_DATE_yyyy-MM-dd");
        params.put("end_date", "END_DATE_yyyy-MM-dd");

        Item item = new Item();
        item.withPrimaryKey("Id", id);
        item.withString("BaseURL", url);
        item.withMap("Headers", headers);
        item.withMap("QueryParams", params);
        item.withString("CompanyName", companyName);
        item.withString("Market", market);
        item.withString("Target", targetTable);
        item.withString("ProductId", productid);
        try {
            PutItemOutcome outcome = table.putItem(item);
            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public static void populateRatingConfig(String url, String productid, String id, String targetTable, String companyName, String market) throws Exception {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2)
                .withCredentials(new ProfileCredentialsProvider("cerebro")).build();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(CONFIG_TABLE);

        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer 978a43de14cf3d36a78458152b6b615e1688b6c1");

        Map<String, String> params = new HashMap<>();
        Item item = new Item();
        item.withPrimaryKey("Id", id);
        item.withString("BaseURL", url);
        item.withMap("Headers", headers);
        item.withMap("QueryParams", params);
        item.withString("CompanyName", companyName);
        item.withString("Market", market);
        item.withString("Target", targetTable);
        item.withString("ProductId", productid);
        try {
            PutItemOutcome outcome = table.putItem(item);
            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public static void populateDemographicConfig(String url, String productid, String id, String targetTable, String companyName, String market) throws Exception {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2)
                .withCredentials(new ProfileCredentialsProvider("cerebro")).build();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(CONFIG_TABLE);

        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer 978a43de14cf3d36a78458152b6b615e1688b6c1");

        Map<String, String> params = new HashMap<>();
        params.put("countries", "US");
        params.put("start_date", "START_DATE_yyyy-MM-dd");
        params.put("end_date", "END_DATE_yyyy-MM-dd");

        Item item = new Item();
        item.withPrimaryKey("Id", id);
        item.withString("BaseURL", url);
        item.withMap("Headers", headers);
        item.withMap("QueryParams", params);
        item.withString("CompanyName", companyName);
        item.withString("Market", market);
        item.withString("Target", targetTable);
        item.withString("ProductId", productid);
        try {
            PutItemOutcome outcome = table.putItem(item);
            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public void iOSApp(String product_id, String appName) throws Exception {
        populateiOSDownloadConfig("https://api.appannie.com/v1.3/intelligence/apps/ios/app/" + product_id + "/history", product_id, UUID.randomUUID().toString(), "Data_Download", appName, "iOS", false);
        populateUsageConfig("https://api.appannie.com/v1.2/intelligence/apps/ios/app/" + product_id + "/usage-history", product_id, UUID.randomUUID().toString(), "Data_Usage", appName, "iOS", false);
        //There is no aggregation on reviews so it's best to download daily
        populateReviewsAPI("https://api.appannie.com/v1.2/apps/ios/app/" + product_id + "/reviews", product_id, UUID.randomUUID().toString(), "Data_Review", appName, "iOS");
        //Ratings always override previous so last record is latest
        populateRatingConfig("https://api.appannie.com/v1.2/apps/ios/app/" + product_id + "/ratings", product_id, UUID.randomUUID().toString(), "Data_Ratings", appName, "iOS");
        //There is no aggregation on demographics so it's best to download daily
        populateDemographicConfig("https://api.appannie.com/v1.2/intelligence/apps/ios/app/" + product_id + "/demographics", product_id, UUID.randomUUID().toString(), "Data_Demographic", appName, "iOS");
    }

    public void googleApp(String product_id, String appName) throws Exception {
        populateAndriodDownloadConfig("https://api.appannie.com/v1.3/intelligence/apps/google-play/app/" + product_id + "/history", product_id, UUID.randomUUID().toString(), "Data_Download", appName, "google-play", false);
        populateUsageConfig("https://api.appannie.com/v1.2/intelligence/apps/all-android/app/" + product_id + "/usage-history", product_id, UUID.randomUUID().toString(), "Data_Usage", appName, "google-play", false);
        //There is no aggregation on reviews so it's best to download daily
        populateReviewsAPI("https://api.appannie.com/v1.2/apps/google-play/app/" + product_id + "/reviews", product_id, UUID.randomUUID().toString(), "Data_Review", appName, "google-play");
        //Ratings always override previous so last record is latest
        populateRatingConfig("https://api.appannie.com/v1.2/apps/google-play/app/" + product_id + "/ratings", product_id, UUID.randomUUID().toString(), "Data_Ratings", appName, "google-play");
        //There is no aggregation on demographics so it's best to download daily
        populateDemographicConfig("https://api.appannie.com/v1.2/intelligence/apps/all-android/app/" + product_id + "/demographics", product_id, UUID.randomUUID().toString(), "Data_Demographic", appName, "google-play");
    }

    public void iOSAppMonthly(String product_id, String appName) throws Exception {
        setMonthlyTable();
        populateiOSDownloadConfig("https://api.appannie.com/v1.3/intelligence/apps/ios/app/" + product_id + "/history", product_id, UUID.randomUUID().toString(), "Data_Download_Monthly", appName, "iOS", true);
        populateUsageConfig("https://api.appannie.com/v1.2/intelligence/apps/ios/app/" + product_id + "/usage-history", product_id, UUID.randomUUID().toString(), "Data_Usage_Monthly", appName, "iOS", true);
        //Cross App is always monthly as App Annie
        populateCrossAppConfig("https://api.appannie.com/v1.2/intelligence/apps/ios/app/" + product_id + "/cross_app_usage", product_id, UUID.randomUUID().toString(), "Data_CrossApp_Monthly", appName, "iOS", null);
        populateCrossAppConfig("https://api.appannie.com/v1.2/intelligence/apps/ios/app/" + product_id + "/cross_app_usage", product_id, UUID.randomUUID().toString(), "Data_CrossApp_Monthly", appName, "iOS", "Overall > Finance");
        populateCrossAppConfig("https://api.appannie.com/v1.2/intelligence/apps/ios/app/" + product_id + "/cross_app_usage", product_id, UUID.randomUUID().toString(), "Data_CrossApp_Monthly", appName, "iOS", "Overall > Business");
    }

    public void googleAppMonthly(String product_id, String appName) throws Exception {
        setMonthlyTable();
        populateAndriodDownloadConfig("https://api.appannie.com/v1.3/intelligence/apps/google-play/app/" + product_id + "/history", product_id, UUID.randomUUID().toString(), "Data_Download_Monthly", appName, "google-play", true);
        populateUsageConfig("https://api.appannie.com/v1.2/intelligence/apps/all-android/app/" + product_id + "/usage-history", product_id, UUID.randomUUID().toString(), "Data_Usage_Monthly", appName, "google-play", true);
        //Cross App is always monthly as App Annie
        populateCrossAppConfig("https://api.appannie.com/v1.2/intelligence/apps/all-android/app/" + product_id + "/cross_app_usage", product_id, UUID.randomUUID().toString(), "Data_CrossApp_Monthly", appName, "google-play", null);
        populateCrossAppConfig("https://api.appannie.com/v1.2/intelligence/apps/all-android/app/" + product_id + "/cross_app_usage", product_id, UUID.randomUUID().toString(), "Data_CrossApp_Monthly", appName, "google-play", "OVERALL > APPLICATION > FINANCE");
        populateCrossAppConfig("https://api.appannie.com/v1.2/intelligence/apps/all-android/app/" + product_id + "/cross_app_usage", product_id, UUID.randomUUID().toString(), "Data_CrossApp_Monthly", appName, "google-play", "OVERALL > APPLICATION > BUSINESS");
    }

}
