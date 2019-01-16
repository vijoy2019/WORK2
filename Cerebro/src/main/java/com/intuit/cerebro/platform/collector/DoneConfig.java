package com.intuit.cerebro.platform.collector;

import com.intuit.cerebro.platform.dynamodb.DynamoDBHandler;
import com.intuit.cerebro.platform.schema.DoneFile;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;

import static com.intuit.cerebro.platform.schema.DoneFile.*;

public class DoneConfig {

    private final MODE mode;

    public DoneConfig() {
        this(MODE.DAY);
    }

    public DoneConfig(MODE mode) {
        this.mode = mode;
    }

    private static Date getNextDate(Date curDate) {
        final Calendar calendar = Calendar.getInstance();
        calendar.setTime(curDate);
        calendar.add(Calendar.DAY_OF_YEAR, 1);
        return calendar.getTime();
    }

    private static Date getNextMonth(Date curDate) {
        final Calendar calendar = Calendar.getInstance();
        calendar.setTime(curDate);
        calendar.add(Calendar.MONTH, 1);
        return calendar.getTime();
    }

    public DoneFile getDoneConfig() throws ParseException {
        DoneFile doneFile = DynamoDBHandler.getInstance().getLastDone();
        if (mode == MODE.DAY) {
            return getDayDoneConfig(doneFile);
        } else {
            return getMonthDoneConfig(doneFile);
        }
    }

    private DoneFile getDayDoneConfig(DoneFile doneFile) throws ParseException {
        Date doneDate = doneFile.getDate();
        Date toDay = getToday();
        if (toDay.equals(doneDate) && doneFile.isDone()) {
            return null;
        }
        if (doneDate.before(toDay) && doneFile.isDone()) {
            Date nextDate = getNextDate(doneDate);
            doneFile.setDate(nextDate);
            doneFile.setDone(false);
            doneFile.setCheckPoint(DEFAULT_CHECK_POINT);
        }
        String appAnnieDate = getAppAnnieDate(doneFile.getDate());
        doneFile.setStartDate(appAnnieDate);
        doneFile.setEndDate(appAnnieDate);
        return doneFile;
    }

    private DoneFile getMonthDoneConfig(DoneFile doneFile) {
        Date doneDate = doneFile.getDate();
        int doneMonth = getMonth(doneDate);
        int currentMonth = getMonth(new Date());
        if (currentMonth == doneMonth && doneFile.isDone()) {
            return null;
        }
        if (doneMonth < currentMonth && doneFile.isDone()) {
            Date nextDate = getNextMonth(doneDate);
            doneFile.setDate(nextDate);
            doneFile.setDone(false);
            doneFile.setCheckPoint(DEFAULT_CHECK_POINT);
        }
        String appAnnieDate = getAppAnnieDate(doneFile.getDate());
        doneFile.setStartDate(appAnnieDate);
        doneFile.setEndDate(appAnnieDate);
        return doneFile;
    }

    private String getLastDayOfMonth(String date) {
        LocalDate convertedDate = LocalDate.parse(date, DateTimeFormatter.ofPattern(APP_ANNIE_FORMAT));
        convertedDate = convertedDate.withDayOfMonth(convertedDate.getMonth().length(convertedDate.isLeapYear()));
        return convertedDate.format(DateTimeFormatter.ofPattern(APP_ANNIE_FORMAT));
    }

    private int getMonth(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        SimpleDateFormat format = new SimpleDateFormat("yyyyMM");
        String yyMMTime = format.format(cal.getTime());
        int month = Integer.parseInt(yyMMTime);
        return month;
    }

    private String getAppAnnieDate(Date date) {
        DateFormat dateFormat = new SimpleDateFormat(APP_ANNIE_FORMAT);
        String appAnnieDate = dateFormat.format(date);
        return appAnnieDate;
    }

    private Date getToday() throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        Date date = new Date();
        String today = dateFormat.format(date);
        Date curDate = dateFormat.parse(today);
        return curDate;
    }

    public enum MODE {
        DAY, MONTH;
    }
}
