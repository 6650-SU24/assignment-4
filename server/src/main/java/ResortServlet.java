
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import model.LiftRideEvent;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


@WebServlet(name = "ResortServlet", value = "/ResortServlet")
public class ResortServlet extends HttpServlet {
    private JedisPool jedisPool;

    private Gson gson = new Gson();

    private DynamoDbClient ddb;

    public void init() throws ServletException {
        ddb = DynamoDbClient.builder()
                .region(Region.US_WEST_2)
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .httpClient(ApacheHttpClient.builder().build())
                .build();

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(100);
        try {
            jedisPool = new JedisPool(poolConfig,
                    "liftrideevents-nlyipt.serverless.usw2.cache.amazonaws.com",
                    6379,
                    15000, // Connection timeout
                    null,
                    true // Use TLS
            );
        } catch (Exception e) {
            throw new ServletException("Failed to initialize Jedis connection and channel pool", e);
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        LiftRideEvent liftRideEvent= new LiftRideEvent();
        res.setContentType("text/plain");
        String urlPath = req.getPathInfo();
        if (urlPath == null || urlPath.isEmpty()) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            res.getWriter().write("missing parameters");
            return;
        }
        String[] urlParts = urlPath.split("/");
        if (!isUrlValid(urlParts,  liftRideEvent)) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            res.getWriter().write("invalid url");
        } else {
//            TODO: handle GET/resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers, parameters already parsed, validated and stored in the variable @liftRideEvent
            getResortSkiersDay(req, res, liftRideEvent);
        }
    }

    private void getResortSkiersDay(HttpServletRequest req, HttpServletResponse res, LiftRideEvent liftRideEvent) throws IOException {
        int resortID = liftRideEvent.getResortID();
        String seasonID = liftRideEvent.getSeasonID();
        String dayID = liftRideEvent.getDayID();
        JsonObject jsonObject = new JsonObject();

        // Constructs the cache key for Redis
        String cacheKey = String.format("resort:%d:season:%s:day:%s:uniqueSkiers", resortID, seasonID, dayID);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            String cachedUniqueSkiers = jedis.get(cacheKey);
            if (cachedUniqueSkiers != null) {
                res.setStatus(HttpServletResponse.SC_OK);
                jsonObject.addProperty("uniqueSkiers", Integer.parseInt(cachedUniqueSkiers));
                res.getWriter().write(gson.toJson(jsonObject));
                return;
            }

            // If not in cache query DynamoDB for skiers
            QueryResponse queryResponse = queryToDB(liftRideEvent);
            Set<String> uniqueSkiers = new HashSet<>();
            for (Map<String, AttributeValue> item : queryResponse.items()) {
                uniqueSkiers.add(item.get("skier-timestamp").s().split("-", 2)[0]);
            }
            int uniqueSkiersCount = uniqueSkiers.size();

            // Updates the cache with the new value
            jedis.set(cacheKey, String.valueOf(uniqueSkiersCount));

            // Returns the unique skiers count to the client
            res.setStatus(HttpServletResponse.SC_OK);
            jsonObject.addProperty("uniqueSkiers", uniqueSkiersCount);
            res.getWriter().write(gson.toJson(jsonObject));
        } catch (Exception e) {
            res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            res.getWriter().write("An error occurred while processing the request: "+ e.getMessage());
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    private QueryResponse queryToDB(LiftRideEvent liftRideEvent) {
        String resortID = String.valueOf(liftRideEvent.getResortID());
        String seasonID = liftRideEvent.getSeasonID();
        String dayID = liftRideEvent.getDayID();

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":resortSeasonDay", AttributeValue.builder().s(String.format("%s-%s-%s",resortID,seasonID,dayID)).build());
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#resortSeasonDay", "resort-season-day");
        String keyConditionExpression = "#resortSeasonDay = :resortSeasonDay";

        QueryRequest queryRequest = QueryRequest.builder()
                .tableName("LiftRide")
                .indexName("resort-season-day-skier-timestamp-index")
                .keyConditionExpression(keyConditionExpression)
                .expressionAttributeValues(expressionAttributeValues)
                .expressionAttributeNames(expressionAttributeNames)
                .build();

        return ddb.query(queryRequest);
    }

    private boolean isUrlValid(String[] urlPath, LiftRideEvent liftRideEvent) {
        // https://app.swaggerhub.com/apis/cloud-perf/SkiDataAPI/2.0#/skiers/writeNewLiftRide

        // /resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers
        if (urlPath.length != 7) return false;

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
        if (!urlPath[4].equals("day")) return false;

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
        return urlPath[6].equals("skiers");
    }
}
