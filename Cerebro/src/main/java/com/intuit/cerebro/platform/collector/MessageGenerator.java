package com.intuit.cerebro.platform.collector;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.util.StringUtils;
import com.intuit.cerebro.platform.dynamodb.DynamoDBHandler;
import com.intuit.cerebro.platform.rest.RESTHandler;
import com.intuit.cerebro.platform.schema.DoneFile;
import com.intuit.cerebro.platform.schema.RequestConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

public class MessageGenerator {

    public MessageGenerator() {

    }

    public void generateMessages(DoneFile doneFile) throws IOException {
        List<RequestConfig> restConfigList = new ArrayList<>();
        String lastItem = DynamoDBHandler.getInstance().fetchConfig(doneFile, restConfigList);
        RESTHandler restHandler = new RESTHandler();
        ResponseHandler responseHandler = new ResponseHandler();
        for (RequestConfig restConfig : restConfigList) {
            enrichRestConfig(restConfig, doneFile);
            String response = restHandler.getResponse(restConfig);
            if (!StringUtils.isNullOrEmpty(response)) {
                responseHandler.handleResponse(response, restConfig);
            } else {
                System.out.println("Got an empty response");
            }
        }
        if (lastItem == null) {
            doneFile.setDone(true);
        } else {
            doneFile.setCheckPoint(lastItem);
        }
        DynamoDBHandler.getInstance().updateDone(doneFile);
    }

    private void enrichRestConfig(RequestConfig restConfig, DoneFile doneFile) {
        Map<String, AttributeValue> params = restConfig.getParams();
        for (Map.Entry<String, AttributeValue> entry : params.entrySet()) {
            String val = entry.getValue().getS();
            final Calendar calendar = Calendar.getInstance();
            calendar.setTime(doneFile.getDate());
            if (val.startsWith("START_DATE")) {
                AttributeValue nValue = new AttributeValue();
                nValue.setS(doneFile.getStartDate());
                entry.setValue(nValue);
                restConfig.setStartDate(String.valueOf(new java.sql.Timestamp(calendar.getTime().getTime())));
            } else if (val.startsWith("END_DATE")) {
                AttributeValue nValue = new AttributeValue();
                nValue.setS(doneFile.getEndDate());
                entry.setValue(nValue);
                restConfig.setEndDate(String.valueOf(new java.sql.Timestamp(calendar.getTime().getTime())));
            }
        }
    }

}
