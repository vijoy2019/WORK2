package com.intuit.cerebro.platform;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;

public class PopulateDoneTable {

    public static void main(String[] args) throws Exception {
        populateDone();
        getItem();
    }

    public static void populateDone() throws Exception {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2)
                .withCredentials(new ProfileCredentialsProvider("cerebro")).build();
        DynamoDB dynamoDB = new DynamoDB(client);
        String tableName = "Config_DoneTable";
        Table table = dynamoDB.getTable(tableName);
        Item item = new Item();
        item.withPrimaryKey("Id", 1);
        item.withString("LastRun", "20171230");
        item.withBoolean("isDone", false);
        item.withString("CheckPoint", "NULL");
        try {
            PutItemOutcome outcome = table.putItem(item);
            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public static void getItem() throws Exception {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2)
                .withCredentials(new ProfileCredentialsProvider("cerebro")).build();
        DynamoDB dynamoDB = new DynamoDB(client);
        String tableName = "Config_DoneTable";
        Table table = dynamoDB.getTable(tableName);

        try {
            GetItemSpec spec = new GetItemSpec().withPrimaryKey("Id", 1);
            Item outcome = table.getItem(spec);
            System.out.println("Got Item");
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

}
