package com.intuit.cerebro.platform.kinesis;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
import com.amazonaws.services.kinesisfirehose.model.Record;

import java.nio.ByteBuffer;
import java.util.List;

public class KinesisHandler {

    private static KinesisHandler handler = null;
    private AmazonKinesisFirehose kinesisClient = null;


    private KinesisHandler() {
        AmazonKinesisFirehoseClientBuilder clientBuilder = AmazonKinesisFirehoseClientBuilder.standard();
        kinesisClient = clientBuilder.build();
    }

    public static KinesisHandler getInstance() {
        if (handler == null) {
            handler = new KinesisHandler();
        }
        return handler;
    }

    public void putRecords(List<String> recordList, String streamName) {
        for (String record : recordList) {
            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setDeliveryStreamName(streamName);
            System.out.println("Copy Data: "+record);
            record = record + "\n";
            Record data = new Record().withData(ByteBuffer.wrap(record.getBytes()));
            putRecordRequest.setRecord(data);
            PutRecordResult result = kinesisClient.putRecord(putRecordRequest);
        }
        System.out.println("Put Result Done");
    }

}
