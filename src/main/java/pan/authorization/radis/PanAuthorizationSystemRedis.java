package pan.authorization.radis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public class PanAuthorizationSystemRedis {

    private static final String REDIS_HOST = "localhost";
    private static final int REDIS_PORT = 6379;

    public static void main(String[] args) {
        PanAuthorizationSystemRedis system = new PanAuthorizationSystemRedis();

        // Simulate adding authorization requests
        new Thread(() -> system.addAuthorizationRequest("pan-1", "auth-1")).start();
        new Thread(() -> system.addAuthorizationRequest("pan-1", "auth-2")).start();
        new Thread(() -> system.addAuthorizationRequest("pan-1", "auth-3")).start();
        
        // Start subscriber threads for different PANs
        new Thread(() -> system.subscribe("pan-1")).start();
    }

    // Add authorization request to Redis queue and notify via Pub/Sub
    public void addAuthorizationRequest(String pan, String auth) {
        try (Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
            String queueKey = "queue:" + pan;
            jedis.lpush(queueKey, auth); // Add authorization to the queue
            jedis.publish("channel:" + pan, "new_request"); // Notify subscribers
            System.out.println("Added authorization: " + auth + " to PAN: " + pan);
        }
    }

    // Subscribe to a PAN channel and process authorization requests
    public void subscribe(String pan) {
        String channel = "channel:" + pan;
        try (Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
            JedisPubSub jedisPubSub = new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    System.out.println("Received message: " + message + " on channel: " + channel);
                    processRequest(pan); // Process the request when notified
                }
            };
            System.out.println("Subscribed to channel: " + channel);
            jedis.subscribe(jedisPubSub, channel); // Block and listen for messages
        }
    }

    // Process requests for a specific PAN
    private void processRequest(String pan) {
        String queueKey = "queue:" + pan;
        String lockKey = "lock:" + pan;
        try (Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
            // Acquire lock for processing
            if (jedis.setnx(lockKey, "locked") == 1) {
                jedis.expire(lockKey, 10); // Set lock expiration to avoid deadlocks
                String auth;
                while ((auth = jedis.rpop(queueKey)) != null) {
                    System.out.println("Processing authorization: " + auth + " for PAN: " + pan);
                    Thread.sleep(2000); // Simulate processing time
                    System.out.println("Completed authorization: " + auth + " for PAN: " + pan);
                }
                jedis.del(lockKey); // Release the lock
            } else {
                System.out.println("Another thread is processing PAN: " + pan);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
