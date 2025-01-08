package testbed;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public class Main {

	private final ConcurrentHashMap<String, ReentrantLock> panLocks = new ConcurrentHashMap<>();

	public static void main(String[] args) {
		Main m = new Main();

		// Start the subscriber in a new thread
		new Thread(m::subscribe).start();

		// Allow the subscriber to initialize
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// Simulate multiple requests for the same PAN
		new Thread(() -> m.handleRequest("pan1", "auth1")).start();
		new Thread(() -> m.handleRequest("pan1", "auth2")).start();
		new Thread(() -> m.handleRequest("pan2", "auth3")).start();
	}

	public void handleRequest(String pan, String auth) {
		System.out.println("Handling request for PAN: " + pan + " with AUTH: " + auth);

		// Acquire the lock for the PAN
		ReentrantLock lock = panLocks.computeIfAbsent(pan, k -> new ReentrantLock());
		lock.lock();
		try (Jedis jedis = new Jedis("localhost", 6379)) {
			// Add `auth` to the list for the given `PAN`
			jedis.rpush(pan, auth);
			System.out.println("Added AUTH: " + auth + " to PAN: " + pan);

			// Publish a notification about the updated `PAN`
			jedis.publish("myChannel", pan);
			System.out.println("Published PAN: " + pan + " to channel");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			lock.unlock();
		}
	}

	public void subscribe() {
		System.out.println("Subscribe process");
		String channel = "myChannel";
		try (Jedis jedis = new Jedis("localhost", 6379)) {
			JedisPubSub jedisPubSub = new JedisPubSub() {
				@Override
				public void onMessage(String channel, String pan) {
					processPan(pan);
				}
			};

			// Subscribe to the channel
			System.out.println("Subscribed to channel: " + channel);
			jedis.subscribe(jedisPubSub, channel);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void processPan(String pan) {
		// Acquire the lock for the PAN
		ReentrantLock lock = panLocks.computeIfAbsent(pan, k -> new ReentrantLock());
		lock.lock();
		try (Jedis jedis = new Jedis("localhost", 6379)) {
			String auth = jedis.lpop(pan);
			if (auth != null) {
				System.out.println("Polled AUTH: " + auth + " from PAN: " + pan);

				long listSize = jedis.llen(pan);
				if (listSize > 0) {
					System.out.println("Remaining list for PAN: " + pan + ": " + jedis.lrange(pan, 0, -1));
					jedis.publish("myChannel", "Updated list for PAN: " + pan);
				} else {
					System.out.println("No remaining AUTH values for PAN: " + pan);
				}
			} else {
				System.out.println("No AUTH values found for PAN: " + pan);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			lock.unlock();
		}
	}
}
