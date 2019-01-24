package com.company;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.IOException;


public class GetItemOpSample {
    static BasicSessionCredentials sessionCredentials = new BasicSessionCredentials(
            "ASIA2AAMV3EATGCX3EOR",
            "mWlmmN5O/jnIKRsOIcGDYzgod7/D1bOw9rRWygNK",
            "FQoGZXIvYXdzEMD//////////wEaDPT3y/DtMicJiGkPlCKTArA83DYVO6qSJIE5HBS17Lu1gQod2bFwpbGEJhhFNU3MLo4Wc8pKt9eixBjptj3woRy/z8+gB+UeY/bfg5gP5Bjlfa1zllbwQNjnjtyvVXQa4Q0Op6MSQ3bPRLgEPY7I9GLopwrZ1jE1Bwmm55rAdO2wmPnrMzrulYj+VvXzTBEOWryoVZL0lnGMYxpqtMymRAp3C/IoliElhnVlL2NA+YmrV97ayNw3n9H0M47ww5hSMf772K9KMvbZW3iyMnCUfly0vaKal6PC0bck8XAw2XHLieEnA8smU5kIseHfEgy026qmzMLgx/f+YAU1j6y8YO8UpnUhFnEFlt66xALTSuH5duI97cnWCHjSayq7sv+h/A+LKLv79eEF");

    static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withRegion("us-west-2")
            .withCredentials(new AWSStaticCredentialsProvider(sessionCredentials))
            .build();

    static DynamoDB dynamoDB = new DynamoDB(client);

    static String tblName = "Data_Usage_Monthly";

    public static void main(String[] args) throws IOException {
        retrieveItem();
    }

    private static void retrieveItem() {
        Table table = dynamoDB.getTable(tblName);
        try {

            // Item item = table.getItem("Company",);
            Item itemCompany = table.getItem("Id","03919c70-a878-4de0-9b29-50595f29667b","Company",null);
            Item itemMarket = table.getItem("Id","03919c70-a878-4de0-9b29-50595f29667b","Market",null);
            Item itemStartDate = table.getItem("Id","03919c70-a878-4de0-9b29-50595f29667b","StartDate",null);
            Item itemProductId = table.getItem("Id","03919c70-a878-4de0-9b29-50595f29667b","ProductId",null);
            Item itemPayload = table.getItem("Id","03919c70-a878-4de0-9b29-50595f29667b","Payload",null);

            System.out.println("Displaying retrieved items...");
            System.out.println(itemCompany.toJSONPretty());
            System.out.println(itemMarket.toJSONPretty());
            System.out.println(itemStartDate.toJSONPretty());
            System.out.println(itemProductId.toJSONPretty());
            System.out.println(itemPayload.toJSONPretty());
        } catch (Exception e) {
            System.err.println("Cannot retrieve items.");
            System.err.println(e.getMessage());
        }
    }
}
