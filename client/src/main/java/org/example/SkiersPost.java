package org.example;

import io.swagger.client.*;
import io.swagger.client.api.SkiersApi;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SkiersPost {
    private static String basePath;
    private static final int TOTAL_REQUESTS = 200000;
    private static final int STARTUP_THREADS = 32;
    private static final int REQUESTS_PER_THREAD = 1000;
    private static final int MAX_RETRIES = 5;
    private static final int SUCCESSCODE = 201;
    private static final String REQUEST_TYPE = "POST";
    private static final BlockingQueue<LiftRideEvent> requestQueue = new LinkedBlockingQueue<>();
    private static final ConcurrentMap<Integer, Record> recordsMap = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Integer, LiftRideEvent> eventMap = new ConcurrentHashMap<>();
    private static final AtomicInteger success = new AtomicInteger(0);
    private static final AtomicInteger failure = new AtomicInteger(0);
    private static final AtomicInteger recordId = new AtomicInteger(0);

    public static void main(String[] args) {
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream("config.properties")) {
            properties.load(fis);
        } catch (IOException e) {
            System.err.println("Error loading config.properties: " + e.getMessage());
            return;
        }
        basePath = properties.getProperty("server.ip");
        if (basePath == null || basePath.isEmpty()) {
            System.err.println("Server IP not configured in config.properties");
            return;
        }
//        System.out.println("read property finish");
        long startTime = System.currentTimeMillis();
        new Thread(() -> {
            for (int i = 0; i < TOTAL_REQUESTS; i++) {
                try {
                    requestQueue.put(LiftRideEvent.generate());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }).start();

//        ExecutorService executorService = Executors.newFixedThreadPool(STARTUP_THREADS);
//        for (int i = 0; i < STARTUP_THREADS; i++) {
//            executorService.submit(() -> sendRequest(REQUESTS_PER_THREAD));
//        }
//        executorService.shutdown();
//        try {
//            if (!executorService.awaitTermination(3, TimeUnit.MINUTES)) {
//                executorService.shutdownNow();
//            }
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        System.out.println("first part finish");
        int remainingPerThread =2500;
//        int remainingReqs = TOTAL_REQUESTS - (STARTUP_THREADS * REQUESTS_PER_THREAD);
        int remainingReqs = TOTAL_REQUESTS;
        int additionalThreads = remainingReqs / remainingPerThread;
        int remainder = remainingReqs % remainingPerThread;
        ExecutorService executorService = Executors.newFixedThreadPool(additionalThreads+ (remainder > 0 ? 1 : 0));
        for (int i = 0; i < additionalThreads; i++) {
            executorService.submit(() -> sendRequest(remainingPerThread));
        }
        if (remainder > 0) {
            executorService.submit(() -> sendRequest(remainder));
        }
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(3, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
//        System.out.println("second part finish");
        long endTime = System.currentTimeMillis();
        double throughput = (double) TOTAL_REQUESTS / ((endTime - startTime) / 1000.0);

        System.out.println("Number of successful requests sent: " + success.get());
        System.out.println("Number of unsuccessful requests: " + failure.get());
        System.out.println("Total run time: " + (endTime - startTime) + " ms");
        System.out.println("Total throughput: " + throughput + " requests per second");

        writeRecords("records.csv");
        calculateStatistics();

    }

    private static void sendRequest(int reqCount) {
        ApiClient apiClient = new ApiClient();
        apiClient.setBasePath(basePath);
        apiClient.setConnectTimeout(100000); // 100 seconds
        apiClient.setReadTimeout(100000);

        SkiersApi apiInstance = new SkiersApi(apiClient);
        LiftRideEvent event = null;
        for (int i = 0; i < reqCount; i++) {
            boolean isSuccess = false;
            int attempts = 0;
            Integer respCode = null;
            long start = System.currentTimeMillis();
            while (!isSuccess && attempts < MAX_RETRIES) {
                try {
                     event = requestQueue.take();
                    apiInstance.writeNewLiftRide(
                            event.getLiftRide(),
                            event.getResortID(),
                            String.valueOf(event.getSeasonID()),
                            String.valueOf(event.getDayID()),
                            event.getSkierID()
                    );

                    success.incrementAndGet();
                    respCode = SUCCESSCODE;
                    isSuccess = true;
                } catch (ApiException e) {
                    attempts++;
                    if (attempts >= MAX_RETRIES) {
                        failure.incrementAndGet();
                        respCode = e.getCode();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            long end = System.currentTimeMillis();
            long latency = end - start;
            int id = recordId.incrementAndGet();
            recordsMap.put(id, new Record(start, REQUEST_TYPE, latency, respCode));
            eventMap.put(id, event);
        }
    }

    private static void writeRecords(String filename) {
        try (FileWriter writer = new FileWriter(filename)) {
//            writer.append("StartTime,RequestType,Latency,ResponseCode\n");
            writer.append("skierID,resortID,seasonID,dayId,liftID,time\n");

//            for (Record record : recordsMap.values()) {
//                writer.append(record.toCsv()).append("\n");
//            }

            for (LiftRideEvent event: eventMap.values()) {
                writer.append(event.toCsv()).append("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void calculateStatistics() {
        long totalLatency = 0;
        long minLatency = Long.MAX_VALUE;
        long maxLatency = Long.MIN_VALUE;
        List<Long> latencies = new ArrayList<>();

        for (Record record : recordsMap.values()) {
            long latency = record.getLatency();
            totalLatency += latency;
            latencies.add(latency);
            if (latency < minLatency) minLatency = latency;
            if (latency > maxLatency) maxLatency = latency;

        }

        long meanLatency = totalLatency / recordsMap.size();
        Collections.sort(latencies);
        long medianLatency = latencies.get(latencies.size() / 2);
        long p99Latency = latencies.get((int) (latencies.size() * 0.99));

        System.out.println("Mean response time: " + meanLatency + " ms");
        System.out.println("Median response time: " + medianLatency + " ms");
        System.out.println("99th percentile response time: " + p99Latency + " ms");
        System.out.println("Min response time: " + minLatency + " ms");
        System.out.println("Max response time: " + maxLatency + " ms");
    }
}

