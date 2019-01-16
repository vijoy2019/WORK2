package com.intuit.cerebro.platform.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.util.StringUtils;
import com.intuit.cerebro.platform.schema.DoneFile;
import com.intuit.cerebro.platform.schema.RequestConfig;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.intuit.cerebro.platform.schema.DoneFile.DATE_FORMAT;
import static com.intuit.cerebro.platform.schema.DoneFile.DEFAULT_CHECK_POINT;

public class DynamoDBHandler {

    private static final String DAY_CONFIG_TABLE = "Config_APIConfig";
    private static final String DAY_DONE_TABLE = "Config_DoneTable";
    private static final String MONTH_CONFIG_TABLE = "Config_APIConfig_Monthly";
    private static final String MONTH_DONE_TABLE = "Config_DoneTable_Monthly";
    public static DynamoDBHandler handler = null;
    private static String CONFIG_TABLE = DAY_CONFIG_TABLE;
    private static String DONE_TABLE = DAY_DONE_TABLE;
    private AmazonDynamoDB client = null;
    private DynamoDB dynamoDB = null;
    private boolean isMonthMode = false;

    private DynamoDBHandler() {
        client = AmazonDynamoDBClientBuilder.standard().build();
        dynamoDB = new DynamoDB(client);
    }

    public static DynamoDBHandler getInstance() {
        if (handler == null) {
            handler = new DynamoDBHandler();
        }
        return handler;
    }

    public void enableMonthMode() {
        isMonthMode = true;
        CONFIG_TABLE = MONTH_CONFIG_TABLE;
        DONE_TABLE = MONTH_DONE_TABLE;
    }

    public DoneFile getLastDone() throws ParseException {
        Table table = dynamoDB.getTable(DONE_TABLE);
        GetItemSpec spec = new GetItemSpec().withPrimaryKey("Id", 1);
        Item outcome = table.getItem(spec);
        DoneFile done = new DoneFile();
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        Date date = null;
        String strDate = outcome.getString("LastRun");
        if (!StringUtils.isNullOrEmpty(strDate)) {
            date = sdf.parse(strDate);
        }
        done.setDate(date);
        done.setDone(outcome.getBoolean("isDone"));
        done.setCheckPoint(outcome.getString("CheckPoint"));
        return done;
    }

    public void updateDone(DoneFile doneFile) {
        Table table = dynamoDB.getTable(DONE_TABLE);
        UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("Id", 1)
                .withUpdateExpression("set LastRun=:r, isDone=:p, CheckPoint=:a")
                .withValueMap(new ValueMap().withString(":r", doneFile.getStrDate()).withBoolean(":p", doneFile.isDone())
                        .withString(":a", doneFile.getCheckPoint()))
                .withReturnValues(ReturnValue.UPDATED_NEW);
        UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
        UpdateItemResult result = outcome.getUpdateItemResult();
        //TODO: Log result
    }

    public String fetchConfig(DoneFile doneFile, List<RequestConfig> restConfigList) {
        Map<String, AttributeValue> lastKeyEvaluated = null;
        if (!doneFile.getCheckPoint().equals(DEFAULT_CHECK_POINT)) {
            lastKeyEvaluated = new HashMap<>();
            AttributeValue value = new AttributeValue();
            value.setS(doneFile.getCheckPoint());
            lastKeyEvaluated.put("Id", value);
        }
        ScanRequest scanRequest = new ScanRequest()
                .withTableName(CONFIG_TABLE)
                .withLimit(10)
                .withExclusiveStartKey(lastKeyEvaluated);
        ScanResult result = client.scan(scanRequest);
        for (Map<String, AttributeValue> item : result.getItems()) {
            RequestConfig restConfig = new RequestConfig();
            restConfig.setBaseUrl(item.get("BaseURL").getS());
            restConfig.setTarget(item.get("Target").getS());
            restConfig.setCompanyName(item.get("CompanyName").getS());
            restConfig.setMarket(item.get("Market").getS());
            restConfig.setProductId(item.get("ProductId").getS());
            Map<String, AttributeValue> headers = item.get("Headers").getM();
            restConfig.getHeaders().putAll(headers);
            Map<String, AttributeValue> params = item.get("QueryParams").getM();
            restConfig.getParams().putAll(params);
            restConfigList.add(restConfig);
        }
        String lastItem = null;
        if (result.getLastEvaluatedKey() != null) {
            AttributeValue id = result.getLastEvaluatedKey().get("Id");
            lastItem = id.getS();
        }
        return lastItem;
    }

    public void saveResponse(String response, RequestConfig restConfig) {
        String tableName = restConfig.getTarget();
        try {
            Table table = dynamoDB.getTable(tableName);
            Item item = new Item();
            item.withPrimaryKey("Id", UUID.randomUUID().toString());
            item.withString("Company", restConfig.getCompanyName());
            item.withString("Market", restConfig.getMarket());
            item.withString("StartDate", restConfig.getStartDate());
            item.withString("EndDate", restConfig.getEndDate());
            item.withString("ProductId", restConfig.getProductId());
            item.withString("Payload", response);
            PutItemOutcome outcome = table.putItem(item);
            PutItemResult result = outcome.getPutItemResult();
        } catch (Exception e) {
            e.printStackTrace();
            //TODO: Add Logs
            System.out.println("Table " + tableName + " Response " + response);
        }
        //TODO: Log result
    }


}
