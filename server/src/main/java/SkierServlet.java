
import javax.servlet.ServletException;
import javax.servlet.http.*;
import javax.servlet.annotation.*;
import java.io.*;
import java.security.InvalidParameterException;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


import com.rabbitmq.client.Channel;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import model.LiftRide;
import model.LiftRideEvent;
import org.apache.commons.lang3.concurrent.EventCountCircuitBreaker;

import rmqpool.RMQChannelFactory;
import rmqpool.RMQChannelPool;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import redis.clients.jedis.Jedis;

@WebServlet(name = "SkierServlet", value = "/SkierServlet")
public class SkierServlet extends HttpServlet {
    private static final String EXCHANGE_NAME = "LiftRide";
    private static final String ROUTING_KEY = "LiftRideKey";
    private static final int MAX_REQUESTS = 4000;
    private static final long TIMEOUT = 1;
    private static final int THRESHOLD = 3500;
    private RMQChannelPool channelPool;
    private Connection connection;

    private Gson gson = new Gson();
    private static EventCountCircuitBreaker circuitBreaker;

    private static final DynamoDbClient ddb = DynamoDbClient.builder()
            .region(Region.US_WEST_2)
            .credentialsProvider(ProfileCredentialsProvider.create())
            .build();


    @Override
    public void init() throws ServletException {
        super.init();
        Properties properties = new Properties();
        try {
            properties.load(getServletContext().getResourceAsStream("/WEB-INF/config.properties"));
        } catch (IOException e) {
            System.err.println("Error loading config.properties: " + e.getMessage());
            return;
        }

        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(properties.getProperty("rmq.host"));
            factory.setPort(Integer.parseInt(properties.getProperty("rmq.port")));
            factory.setUsername(properties.getProperty("rmq.username"));
            factory.setPassword(properties.getProperty("rmq.pwd"));
            this.connection = factory.newConnection();

            RMQChannelFactory channelFactory = new RMQChannelFactory(connection);
            this.channelPool = new RMQChannelPool(100, channelFactory);


        } catch (Exception e) {
            throw new ServletException("Failed to initialize RabbitMQ connection and channel pool", e);
        }

        circuitBreaker =
                new EventCountCircuitBreaker(MAX_REQUESTS, TIMEOUT, TimeUnit.SECONDS, THRESHOLD);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        LiftRideEvent liftRideEvent = new LiftRideEvent();
        res.setContentType("text/plain");
        String urlPath = req.getPathInfo();
        if (urlPath == null || urlPath.isEmpty()) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            res.getWriter().write("missing parameters");
            return;
        }
        String[] urlParts = urlPath.split("/");
        if (!isUrlValid(urlParts, liftRideEvent)) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            res.getWriter().write("invalid url");
        } else {
            if (urlParts[2].equals("vertical")) {
                getTotalVertical(req, res, liftRideEvent);
            } else {
                getDayVertical(req, res, liftRideEvent);
            }
        }
    }


    private void getTotalVertical(HttpServletRequest req, HttpServletResponse res, LiftRideEvent liftRideEvent) throws IOException {
        // TODO: handle GET/skiers/{skierID}/vertical, parameters already parsed, validated and stored in the variable @liftRideEvent
        res.setStatus(HttpServletResponse.SC_OK);
        res.getWriter().write("Handled getTotalVertical: skierID=" + liftRideEvent.getSkierID());
    }

    private void getDayVertical(HttpServletRequest req, HttpServletResponse res, LiftRideEvent liftRideEvent) throws IOException {
        // TODO: handle GET/skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID},parameters already parsed, validated and stored in the variable @liftRideEvent
        int skierID = liftRideEvent.getSkierID();
        int resortID = liftRideEvent.getResortID();
        String seasonID = liftRideEvent.getSeasonID();
        String dayID = liftRideEvent.getDayID();

        // Checks the cache for the vertical first
        Jedis jedis = new Jedis("liftrideevents-lpt25z.serverless.usw2.cache.amazonaws.com:6379", 6379);
        String cacheKey = String.format("skier:%d:resort:%d:season%s:day:%s:vertical", skierID, resortID, seasonID, dayID);
        String cachedVertical = jedis.get(cacheKey);

        if (cachedVertical != null) {
            res.setStatus(HttpServletResponse.SC_OK);
            res.getWriter().write("Total vertical: " + cachedVertical);
            jedis.close();
            return;
        }

        // Queries DynamoDB for lift rides if the cache is empty
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":skierID", AttributeValue.builder().n(String.valueOf(skierID)).build());
        expressionAttributeValues.put(":resortID", AttributeValue.builder().n(String.valueOf(resortID)).build());
        expressionAttributeValues.put(":seasonID", AttributeValue.builder().s(String.valueOf(seasonID)).build());
        expressionAttributeValues.put(":dayID", AttributeValue.builder().s(String.valueOf(dayID)).build());

        QueryRequest queryRequest = QueryRequest.builder()
                .tableName("LiftRide")
                .keyConditionExpression("skierID = :skierID and resortID = :resortID and seasonID = :seasonID and dayID = :dayID")
                .expressionAttributeValues(expressionAttributeValues)
                .build();

        QueryResponse queryResponse = ddb.query(queryRequest);

        // Calculates the total vertical
        int totalVertical = 0;
        for (Map<String, AttributeValue> item : queryResponse.items()) {
            int liftID = Integer.parseInt(item.get("liftID").n());
            totalVertical += liftID * 10;
        }

        // Writes the result to the cache
        jedis.set(cacheKey, String.valueOf(totalVertical));

        // Closes the jedis connection
        jedis.close();

        // Sends response to the client
        res.setStatus(HttpServletResponse.SC_OK);
        res.getWriter().write("Handled getDayVertical: skierID=" + liftRideEvent.getSkierID());
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        LiftRideEvent liftRideEvent = new LiftRideEvent();
        res.setContentType("text/plain");
        String urlPath = req.getPathInfo();
        if (urlPath == null || urlPath.isEmpty()) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            res.getWriter().write("missing parameters");
            return;
        }
        String[] urlParts = urlPath.split("/");
        if (!isUrlValid(urlParts, liftRideEvent)) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            res.getWriter().write("invalid url");
            return;
        }

        StringBuilder reqBody = new StringBuilder();
        try (BufferedReader reader = req.getReader()) {
            String line;
            while ((line = reader.readLine()) != null) {
                reqBody.append(line);
            }
            isReqBodyValid(reqBody.toString(), liftRideEvent);
        } catch (IOException e) {
            res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            res.getWriter().write("error reading request body");
            return;
        } catch (Exception e) {
            res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            res.getWriter().write(e.getMessage());
            return;
        }

        String message = gson.toJson(liftRideEvent);

        boolean success = false;
        int retries = 0;
        int maxRetries = 10;
        long backoffTime = 1000;

        Channel channel = channelPool.borrowObject();

        try {
            while (!success && retries < maxRetries) {
                if (circuitBreaker.incrementAndCheckState()) {
                    try {
                        channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, null, message.getBytes());
                        res.setStatus(HttpServletResponse.SC_CREATED);
                        res.getWriter().write("Success - New lift ride for skierID " + liftRideEvent.getSkierID());
                        success = true;
                    } catch (Exception e) {
                        retries++;
                        backoffTime = Math.min(backoffTime * 2, 16000);
                        try {
                            Thread.sleep(backoffTime);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                } else {
                    retries++;
                    backoffTime = Math.min(backoffTime * 2, 16000);
                    try {
                        Thread.sleep(backoffTime);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }

        } finally {
            if (!success) {
                res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                res.getWriter().println("Please try again later.");
            }

            if (channel != null) {
                try {
                    channelPool.returnObject(channel);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }


    private void isReqBodyValid(String reqBody, LiftRideEvent liftRideEvent) {
        JsonObject jsonObject = gson.fromJson(reqBody.toString(), JsonObject.class);
        if (jsonObject.has("time") && jsonObject.has("liftID")) {
            int time = jsonObject.get("time").getAsInt();
            int liftID = jsonObject.get("liftID").getAsInt();
            if (time < 1 || time > 360 || liftID < 1 || liftID > 40) {
                throw new InvalidParameterException("invalid time or liftID");
            } else {
                liftRideEvent.setLiftRide(new LiftRide(time, liftID));
                liftRideEvent.setTimestamp();
            }

        } else {
            throw new InvalidParameterException("missing time or liftID");
        }
    }


    private boolean isUrlValid(String[] urlPath, LiftRideEvent liftRideEvent) {
        // https://app.swaggerhub.com/apis/cloud-perf/SkiDataAPI/2.0#/skiers/writeNewLiftRide
        // /skiers/{skierID}/vertical
        if (urlPath.length == 3 && urlPath[2].equals("vertical")) {
            try {
                int skierID = Integer.parseInt(urlPath[1]);
                if (skierID < 1 || skierID > 100000) {
                    return false;
                } else {
                    liftRideEvent.setSkierID(skierID);
                    return true;
                }
            } catch (NumberFormatException e) {
                return false;
            }
        }

        // /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
        // urlPath  = "/1/seasons/2019/days/1/skiers/123"
        // urlParts = [, 1, seasons, 2019, day, 1, skier, 123]
//      // Data range rule refer to Assignment 1
        if (urlPath.length != 8) return false;

        // Check resortID
        try {
            int resortID = Integer.parseInt(urlPath[1]);
            if (resortID < 1 || resortID > 10) {
                return false;
            } else {
                liftRideEvent.setResortID(resortID);
            }
        } catch (NumberFormatException e) {
            return false;
        }

        // Check seasons
        if (!urlPath[2].equals("seasons")) return false;

        liftRideEvent.setSeasonID(urlPath[3]);

        // Check days
        if (!urlPath[4].equals("days")) return false;

        // Check dayID
        try {
            int dayID = Integer.parseInt(urlPath[5]);
            if (dayID < 1 || dayID > 366) {
                return false;
            } else {
                liftRideEvent.setDayID(Integer.toString(dayID));
            }
        } catch (NumberFormatException e) {
            return false;
        }

        // Check skiers
        if (!urlPath[6].equals("skiers")) return false;

        // Check skierID
        try {
            int skierID = Integer.parseInt(urlPath[7]);
            if (skierID < 1 || skierID > 100000) {
                return false;
            } else {
                liftRideEvent.setSkierID(skierID);
            }
        } catch (NumberFormatException e) {
            return false;
        }

        return true;
    }
}
