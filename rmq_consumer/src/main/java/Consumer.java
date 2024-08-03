import com.rabbitmq.client.*;
import model.LiftRideEvent;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;


public class Consumer {
    private static final String EXCHANGE_NAME = "LiftRide";
    private static final String TABLE_NAME = "LiftRide";

    private static final String QUEUE_NAME = "LiftRideQueue";
    private static final String ROUTING_KEY = "LiftRideKey";
    private static final int THREAD_POOL_SIZE = 200;
    private final static int BATCH_SIZE = 25;

    private static final Gson gson = new Gson();


    private static final DynamoDbClient ddb = DynamoDbClient.builder()
            .region(Region.US_WEST_2)
            .credentialsProvider(ProfileCredentialsProvider.create())
            .build();
    public static void main(String[] args) throws InterruptedException {

        Properties properties = new Properties();
        try (InputStream fis = Consumer.class.getClassLoader().getResourceAsStream("config.properties")) {
            properties.load(fis);
        } catch (IOException e) {
            System.err.println("Error loading config.properties: " + e.getMessage());
            return;
        }

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(properties.getProperty("rmq.host"));
        factory.setPort(Integer.parseInt(properties.getProperty("rmq.port")));
        factory.setUsername(properties.getProperty("rmq.username"));
        factory.setPassword(properties.getProperty("rmq.pwd"));
        factory.setRequestedHeartbeat(30);
        factory.setAutomaticRecoveryEnabled(true);

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);


        try {
            Connection connection = factory.newConnection();
            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
            for (int i = 0; i < THREAD_POOL_SIZE; i++) {
                executor.submit(()-> {
                    try {
                        Channel channel = connection.createChannel();
                        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
                        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
                        channel.basicQos(BATCH_SIZE);

                        List<Map<String, AttributeValue>> items = Collections.synchronizedList(new ArrayList<>());

                        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                            String message = new String(delivery.getBody(), "UTF-8");
                            LiftRideEvent event = gson.fromJson(message, LiftRideEvent.class);

                            Map<String, AttributeValue> item = Map.of(
                                    "skierID", AttributeValue.builder().n(String.valueOf(event.getSkierID())).build(),
                                    "timestamp", AttributeValue.builder().s(event.getTimestamp()).build(),
                                    "resortID", AttributeValue.builder().n(String.valueOf(event.getResortID())).build(),
                                    "liftID", AttributeValue.builder().n(String.valueOf(event.getLiftRide().getLiftID())).build(),
                                    "seasonID", AttributeValue.builder().n(String.valueOf(event.getSeasonID())).build(),
                                    "dayID", AttributeValue.builder().n(String.valueOf(event.getDayID())).build(),
                                    "time", AttributeValue.builder().n(String.valueOf(event.getLiftRide().getTime())).build());

                            items.add(item);
                            if (items.size() >= BATCH_SIZE) {
                                writeBatchToDB(ddb, items);
                                items.clear();
                            }
                        };

                        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});

                        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
                            if (!items.isEmpty()) {
                                writeBatchToDB(ddb, items);
                                items.clear();
                            }
                        }, 0, 1, TimeUnit.MINUTES);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            synchronized (Consumer.class) {
                Consumer.class.wait();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }

    }


        private static void writeBatchToDB(DynamoDbClient ddb, List<Map<String, AttributeValue>> items) {
            List<WriteRequest> writeRequests = items.stream()
                    .map(item -> WriteRequest.builder()
                            .putRequest(PutRequest.builder()
                                    .item(item)
                                    .build())
                            .build())
                    .collect(Collectors.toList());

            Map<String, List<WriteRequest>> requestItems = new HashMap<>();
            requestItems.put(TABLE_NAME, writeRequests);

            BatchWriteItemRequest batchRequest = BatchWriteItemRequest.builder()
                    .requestItems(requestItems)
                    .build();

            int attempts = 0;
            boolean success = false;
            long backoffTime = 1000;

            while (!success && attempts < 5) {
                try {
                    BatchWriteItemResponse result = ddb.batchWriteItem(batchRequest);
                    if (result.hasUnprocessedItems() && !result.unprocessedItems().isEmpty()) {
                        requestItems = result.unprocessedItems();
                        batchRequest = BatchWriteItemRequest.builder()
                                .requestItems(requestItems)
                                .build();
                        attempts++;
                        try {
                            Thread.sleep( Math.min(backoffTime * 2, 16000));
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    } else {
                        success = true;
                    }
                } catch ( SdkClientException | AwsServiceException e) {
                    e.printStackTrace();
                        attempts++;
                        try {
                            Thread.sleep( Math.min(backoffTime * 2, 16000));
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                }
            }
            if (!success) {
                System.err.println("Failed to write batch to DynamoDB after 5 attempts");
            }
        }
}

