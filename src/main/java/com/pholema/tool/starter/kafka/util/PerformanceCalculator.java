package com.pholema.tool.starter.kafka.util;

import java.lang.management.ManagementFactory;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.pholema.tool.utils.common.DateUtils;

public class PerformanceCalculator extends TimerTask {

    private static Logger logger = Logger.getLogger(PerformanceCalculator.class);

    private static final long MEGABYTE = 1024L * 1024L;
    private static final DecimalFormat df = new DecimalFormat("#.##");
    private static Runtime runtime = Runtime.getRuntime();
    private static Map<String,Long> pollingRecordbyHour = new HashMap<>();
    private static Map<String,Long> successRecordbyHour = new HashMap<>();
    private static String PID;
    private static Timer timer;
    
    private static AtomicInteger totalcount =new AtomicInteger();
    private static AtomicInteger totalcountSuccess =new AtomicInteger();

    public static void started() {
        if (timer == null) {
            long PERIOD_TIME = 5 * 60 * 1000;
            timer = new Timer();
            timer.schedule(new PerformanceCalculator(), 0, PERIOD_TIME);
        }
    }

    public static void printLogExecutedTime(String whno, String recordCount, long startTime) {
        long stopTime = System.currentTimeMillis();
        logger.info(getPID() + " poll " + whno+ " " + recordCount + " records and execute " + (stopTime - startTime) + "milliseconds");
    }

    public static void plus(int recordCount) {
        if (recordCount == 0) {
            return;
        }
        String dateOnHour = DateUtils.getDateTime("yyyyMMdd/HH");
        Long r = pollingRecordbyHour.get(dateOnHour);
        if (r == null) {
            pollingRecordbyHour.put(dateOnHour, (long) recordCount);
        } else {
            pollingRecordbyHour.put(dateOnHour, r + recordCount);
        }

        started();
        totalcount.addAndGet(recordCount);

    }
    
    public static void plusSuccess(int recordCount) {
        if (recordCount == 0) {
            return;
        }
        String dateOnHour = DateUtils.getDateTime("yyyyMMdd/HH");
        Long r = successRecordbyHour.get(dateOnHour);
        if (r == null) {
        	successRecordbyHour.put(dateOnHour, (long) recordCount);
        } else {
        	successRecordbyHour.put(dateOnHour, r + recordCount);
        }
        totalcountSuccess.addAndGet(recordCount);
    }

    public static void printLogPollingRecordbyHour() {
        for (Map.Entry entry : pollingRecordbyHour.entrySet()) {
            logger.info("all polling record:"+getPID() + " date on :" + entry.getKey() + " -> " + entry.getValue());
        }
        logger.info("all polling record:"+getPID() + " total count :" + totalcountSuccess.get());
        logger.info("=============================================");
        for (Map.Entry entry : successRecordbyHour.entrySet()) {
            logger.info("success :" +getPID() + " date on :" + entry.getKey() + " -> " + entry.getValue());
        }
        logger.info("all polling record:"+getPID() + " total count :" + totalcount.get());
        logger.info("=============================================");
        printLogUsedMemory();
    }

    public static void printLogUsedMemory() {
        // Run the garbage collector
        runtime.gc();
        // Calculate the used memory
        long memory = runtime.totalMemory() - runtime.freeMemory();
        logger.info(getPID() + " Used memory is bytes: " + memory + ", megabytes:" + df.format((double) memory / MEGABYTE) + " " + df.format(100 * (double) memory / runtime.maxMemory()) + "%");
    }

    public static String getPID() {
        if (PID == null) {
            PID = ManagementFactory.getRuntimeMXBean().getName();
        }
        return PID;
    }

    @Override
    public void run() {
        printLogPollingRecordbyHour();
    }
}
