package rmqpool;

import com.rabbitmq.client.Channel;
import org.apache.commons.pool2.ObjectPool;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.pool2.impl.GenericObjectPool;


/**
 * A simple RabbitMQ channel pool based on a BlockingQueue implementation
 *
 */
public class RMQChannelPool {
    private static final String EXCHANGE_NAME = "LiftRide";
    private static final String QUEUE_NAME = "LiftRideQueue";
    private static final String ROUTING_KEY = "LiftRideKey";
    // used to store and distribute channels
    private final BlockingQueue<Channel> pool;
    // fixed size pool
    private int capacity;
    // used to ceate channels
    private RMQChannelFactory factory;



    public RMQChannelPool(int maxSize, RMQChannelFactory factory) {
        this.capacity = maxSize;
        pool = new LinkedBlockingQueue<>(capacity);
        this.factory = factory;
        for (int i = 0; i < capacity; i++) {
            Channel chan;
            try {
                chan = factory.create();
                chan.queueDeclare(QUEUE_NAME, true, false, false, null);
                chan.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
                pool.put(chan);
            } catch (IOException | InterruptedException ex) {
                Logger.getLogger(RMQChannelPool.class.getName()).log(Level.SEVERE, null, ex);
            }

        }
    }

    public Channel borrowObject() throws IOException {

        try {
            return pool.take();
        } catch (InterruptedException e) {
            throw new RuntimeException("Error: no channels available" + e.toString());
        }
    }

    public void returnObject(Channel channel) throws Exception {
        if (channel != null) {
            pool.add(channel);
        }
    }

    public void close() {
        // pool.close();
    }
}