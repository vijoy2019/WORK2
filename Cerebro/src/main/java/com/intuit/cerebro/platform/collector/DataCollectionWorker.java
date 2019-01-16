package com.intuit.cerebro.platform.collector;

import com.intuit.cerebro.platform.schema.DoneFile;

import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;

public class DataCollectionWorker {

    public DataCollectionWorker() {

    }

    public void doDataCollection() throws ParseException , IOException {
        DoneConfig doneConfig = new DoneConfig();
        dataCollection(doneConfig);
    }

    public void doDataCollection(DoneConfig.MODE mode) throws ParseException , IOException {
        DoneConfig doneConfig = new DoneConfig(mode);
        dataCollection(doneConfig);
    }

    private void dataCollection (DoneConfig doneConfig) throws ParseException , IOException {
        DoneFile doneFile = doneConfig.getDoneConfig();
        if (doneFile != null) {
            MessageGenerator messageGenerator = new MessageGenerator();
            messageGenerator.generateMessages(doneFile);
        } else {
            System.out.println("No data run for today "+ Calendar.getInstance().getTime().toString());
        }
    }

}
