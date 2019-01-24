package com.company;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DynamoDBToJson {

    public static void main(String[] args) throws Exception{

        /*
        BasicSessionCredentials sessionCredentials = new BasicSessionCredentials(
                "ASIA2AAMV3EASLNRKETX",
                "qasV4itvzYAEH1308m/gVcBB0Cdp3vDyahICTbcx",
                "FQoGZXIvYXdzEMb//////////wEaDHhi73hriwaepx5yBCKTAuv4ZtS95AUXVxhgVFCuXMCfVEE2mLUMdTY/vtjYsSHSqOS4+lZy/DWSvtZWNj6BFC84XijQ7F8wRoxCJTVcjV284mthg8kQ8thQ/fbCN95oO/xkkXbCHJpxUD8eIr1Kd/sACnWTQAokUPsvma+JTe3Qpyakv064PzRoH+EMKlykG+DMIpqAAKFaO3BNEE7iHrU9iVEnBguFKauC/Jkc801VUV1R5X+J9q9F1Imo7CA347Ig262J3PdvNtREq3vZnaDVx7baY9cOMS+b7/dY1hshhi/gjmvci+7ASGRWW3fMIPNaNLLnn5He9VZozjqhYht2WU1PAp9wPXgVZhjLyzQT7vtvACqM9qtk3bIGirIKNXXwKJ6x9+EF");

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion("us-west-2")
                .withCredentials(new AWSStaticCredentialsProvider(sessionCredentials))
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);


        TableCollection<ListTablesResult> tables = dynamoDB.listTables();
        Iterator<Table> iterator = tables.iterator();

        while (iterator.hasNext()) {
            Table table = iterator.next();
            System.out.println(table.getTableName());
        }
        */

        // getM()
        // format(Map<"Data_Usage_Monthly",Company>);

        //String tableName = "Data_Usage_Monthly";
        //Table table = dynamoDB.getTable(tableName);

        String table_name = "Data_Usage_Monthly";
        String name = "03919c70-a878-4de0-9b29-50595f29667b";
        //String projection_expression = null;
        String projection_expression = "Company,Market,StartDate,ProductId,Payload";

/*
        HashMap<String, AttributeValue> key_to_get = new HashMap<String, AttributeValue>();
        key_to_get.put("Id", new AttributeValue(name));

         GetItemRequest request = null;
        if (projection_expression != null) {
            request = new GetItemRequest()
                    .withKey(key_to_get)
                    .withTableName(table_name)
            .withProjectionExpression(projection_expression);
        } else {
            request = new GetItemRequest()
                    .withKey(key_to_get)
                    .withTableName(table_name);
        }

        //final AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();

        try {
            Map<String, AttributeValue> returned_item =
                    client.getItem(request).getItem();
            if (returned_item != null) {
                Set<String> keys = returned_item.keySet();
                for (String key : keys) {
                    System.out.format("%s: %s\n",
                            key, returned_item.get(key).toString());
                }
            } else {
                 System.out.format("No item found with the key %s!\n", name);
            }
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
*/
        fetchItems();
    }


    static BasicSessionCredentials sessionCredentials = new BasicSessionCredentials(
            "ASIA2AAMV3EAT3YBSBOV",
            "iTv3XeA/WLp+03/IlHid4+21VTrfytzy2ZJlY5iN",
            "FQoGZXIvYXdzEO///////////wEaDBLnj34/y+Az9uEA6iKTAjU0+A8PAVA6TLytqMOxYV7CB/+4MrWN9xV1YmL8LQg78PV/VdqLhqhP661jmnh/3aQsWzOTQm5WWaTM75wG4HtB5+GQccUhDGPYv0JZImF5Q7Vcy2C6TXtIduPZVbLS6d5ffuXxSRBGFclqb3frwh3CaejBELzInAsP1fJ24HysKa6DaVyqm711U+jlID8lZO8oTUHl8GSk4L3i3+YriNDMLeHXvznFBgvaTZws4C04qDyYcdEjqg8wNjL4ZmGjShgfX10pOCsXryK+6IRbyMhP26QiR2m7at1d1h0ywVA/BQvLfzIY6De9bfFEyDzLSd+TnAfzf3NKHWmbi95NZdE4rZEByzfiYYe4ag8rlz1pMoooKMSwgOIF");

    static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withRegion("us-west-2")
            .withCredentials(new AWSStaticCredentialsProvider(sessionCredentials))
            .build();

    DynamoDB dynamoDB = new DynamoDB(client);
    static String tableName = "Data_Usage_Monthly";

    private static ArrayList<String> fetchItems() throws IOException{

        ArrayList<String> ids = new ArrayList<String>();

        ScanResult result = null;


        // String fileContent = "Hello Learner !! Welcome to howtodoinjava.com.";
try (
    BufferedWriter writer = new BufferedWriter(
            new FileWriter("/Users/pswain1/workspace/dynamoreader/src/main/resources/output.json"))) {

    do {
        String projection_expression = "Company,Market,StartDate,ProductId,Payload";
        ScanRequest req = new ScanRequest().withProjectionExpression(projection_expression);
        req.setTableName(tableName);

        if (result != null) {
            req.setExclusiveStartKey(result.getLastEvaluatedKey());
        }

        result = client.scan(req);

        List<Map<String, AttributeValue>> rows = result.getItems();

        System.out.println("size" + rows.size());

        rows.forEach(x ->
        {
            try {
                String fileContent = DynamoDBToJson.format(x);
                writer.write(fileContent);
                writer.newLine();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        });


           /* for (Map<String, AttributeValue> map : rows) {
                try {
                    //Company,Market,StartDate,ProductId,Payload
                    AttributeValue v1 = map.get("Company");
                    AttributeValue v2 = map.get("Market");
                    AttributeValue v3 = map.get("StartDate");
                    AttributeValue v4 = map.get("ProductId");
                    AttributeValue v5 = map.get("Payload");
                    String id1 = v1.getS();
                    String id2 = v2.getS();
                    String id3 = v3.getS();
                    String id4 = v4.getS();
                    String id5 = v5.getS();
                    ids.add(id1);
                    System.out.println(id1 + "*********" + id2 + "************" + id3 + "********" + id4 + "************" + id5);
                } catch (NumberFormatException e) {
                    System.out.println(e.getMessage());
                }
            }*/
    } while (result.getLastEvaluatedKey() != null);

    System.out.println("Result size: " + ids.size());
    return ids;
}
    }



        public static String format (Map < String, AttributeValue > values) throws JsonProcessingException {
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
    public static void parseResponse(UsageSchema schema, String payLoad) {
        JsonObject jsonObject = new JsonParser().parse(payLoad).getAsJsonObject();
        JsonArray arr = jsonObject.getAsJsonArray("list");
        BigDecimal active_users = BigDecimal.ZERO;
        //BigInteger active_users = BigInteger.ZERO;
        double avg_sessions_per_user = 0;
        double avg_time_per_user = 0;
        double usage_penetration = 0;
        double install_penetration = 0;
        BigDecimal install_base = BigDecimal.ZERO;
        //BigInteger install_base = BigInteger.ZERO;
        for (int i = 0; i < arr.size(); i++) {
            JsonElement jsonEle = arr.get(i);
            //active_users = active_users + getValue(jsonEle, "active_users");
            //active_users = active_users.add(new BigDecimal(getStrValue(jsonEle, "active_users")));
            active_users = active_users.add(new BigDecimal(getStrValue(jsonEle, "active_users")));
            avg_sessions_per_user = avg_sessions_per_user + getValue(jsonEle, "avg_sessions_per_user");
            avg_time_per_user = avg_time_per_user + getValue(jsonEle, "avg_time_per_user");
            usage_penetration = usage_penetration + getValue(jsonEle, "usage_penetration");
            install_penetration = install_penetration + getValue(jsonEle, "install_penetration");
        }
        if (usage_penetration != 0 && install_penetration != 0) {
            /*BigInteger bd = BigDecimal.valueOf(usage_penetration).setScale(5,BigDecimal.ROUND_DOWN).
                    toBigInteger();*/
            BigDecimal bd = BigDecimal.valueOf(usage_penetration).setScale(5,BigDecimal.ROUND_DOWN);
           // bd.setScale(8, BigDecimal.ROUND_HALF_DOWN);
           // BigInteger value = bd.toBigInteger();
            //BigDecimal div = active_users.divide(bd);
            install_base = active_users.divide(bd, 8, BigDecimal.ROUND_DOWN).
                    multiply(BigDecimal.valueOf(install_penetration).
                            setScale(8,BigDecimal.ROUND_HALF_DOWN));
        }
        //schema.setActive_users(String.valueOf(active_users));
        schema.setActive_users(String.valueOf(active_users));
        schema.setAvg_sessions_per_user(String.valueOf(avg_sessions_per_user));
        schema.setAvg_time_per_user(String.valueOf(avg_time_per_user));
        schema.setUsage_penetration(String.valueOf(usage_penetration));
        schema.setInstall_penetration(String.valueOf(install_penetration));
        schema.setInstall_base(String.valueOf(install_base));
    }

    public static String getStrValue(JsonElement jsonEle, String elementName) {
        String dVal = "0";
        JsonElement jsonElement = jsonEle.getAsJsonObject().get(elementName);
        if (jsonElement != null && !jsonElement.isJsonNull()) {
            String sVal = jsonElement.getAsString();
            return sVal;
        }
        return dVal;
    }

    public static double getValue(JsonElement jsonEle, String elementName) {
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

