package com.intuit.cerebro.platform.schema;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DoneFile {
    public static final String DEFAULT_CHECK_POINT = "NULL";
    public static final String DATE_FORMAT = "yyyyMMdd";
    public static final String APP_ANNIE_FORMAT = "yyyy-MM-dd";
    private Date date;
    private boolean isDone;
    private String CheckPoint;
    private String startDate;
    private String endDate;

    public DoneFile() { }

    public Date getDate() { return date; }

    public void setDate(Date date) { this.date = date; }

    public boolean isDone() { return isDone; }

    public void setDone(boolean done) { isDone = done; }

    public String getCheckPoint() { return CheckPoint; }

    public void setCheckPoint(String checkPoint) { CheckPoint = checkPoint; }

    public String getStartDate() { return startDate; }

    public void setStartDate(String startDate) { this.startDate = startDate; }

    public String getEndDate() { return endDate; }

    public void setEndDate(String endDate) { this.endDate = endDate; }

    public String getStrDate() {
        DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        String date = dateFormat.format(getDate());
        return date;
    }

}
