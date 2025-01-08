package pan.authorization.radis.processor;

import java.util.UUID;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

public class PanAuthorizationProcessor {

	private static final String REDIS_HOST = "localhost";
	private static final int REDIS_PORT = 6379;

	public void execute(String pan, String auth, long threadSleep) {
		String lockKey = "lock:" + pan;
		String queueKey = "queue:" + pan;
		String lockValue = UUID.randomUUID().toString(); // Unique lock identifier for this thread
		String str = pan + " : " + auth;

		try (Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
			// Step 1: Add the authorization to the queue
			jedis.lpush(queueKey, auth);

			// Step 2: Perform method-1 and method-2
			method1(str);
			method2(str);

			// Step 3: Wait for authorization at the head of the queue and acquire lock
			boolean isAuthorized = false;
			while (!isAuthorized) {
				synchronized (this) {
					String currentAuth = jedis.lindex(queueKey, -1); // Peek the last element
					if (auth.equals(currentAuth) 
//							&& acquireLock(jedis, lockKey, lockValue)
							) {
						isAuthorized = true; // Authorization acquired
					}
//                    else {
//                        // Wait before retrying
//                        Thread.sleep(threadSleep);
//                    }
				}
			}

			try {
				// Step 4: Perform method-3, method-4, and method-5
				method3(str, threadSleep);
				method4(str, threadSleep);
				method5(str, threadSleep);

				// Step 5: Poll the current auth from the queue
				jedis.rpop(queueKey);
			} finally {
				// Step 6: Release the lock
				releaseLock(jedis, lockKey, lockValue);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Step 7: Perform method-6
		method6(str);
	}

	private boolean acquireLock(Jedis jedis, String lockKey, String lockValue) {
		SetParams params = SetParams.setParams().nx().px(10000); // NX for "set if not exists", PX for expiration
		String result = jedis.set(lockKey, lockValue, params);
		return "OK".equals(result); // "OK" indicates the lock was successfully acquired
	}

	private void releaseLock(Jedis jedis, String lockKey, String lockValue) {
		// Ensure the lock is still owned by this thread before releasing
		String currentValue = jedis.get(lockKey);
		if (lockValue.equals(currentValue)) {
			jedis.del(lockKey);
		}
	}

	// Sample methods to simulate the operations
	private void method1(String str) {
		System.out.println("Enter method-1");
		System.out.println("Executing method-1 : " + str);
		System.out.println("Exit method-1");
	}

	private void method2(String str) {
		System.out.println("Enter method-2");
		System.out.println("Executing method-2 : " + str);
		System.out.println("Exit method-2");
	}

	private void method3(String str, long sleep) throws InterruptedException {
		System.out.println("Enter method-3");
		System.out.println("Executing method-3 : " + str);
		Thread.sleep(sleep);
		System.out.println("Exit method-3");
	}

	private void method4(String str, long sleep) throws InterruptedException {
		System.out.println("Enter method-4");
		System.out.println("Executing method-4 : " + str);
		Thread.sleep(sleep);
		System.out.println("Exit method-4");
	}

	private void method5(String str, long sleep) throws InterruptedException {
		System.out.println("Enter method-5");
		System.out.println("Executing method-5 : " + str);
		Thread.sleep(sleep);
		System.out.println("Exit method-5");
	}

	private void method6(String str) {
		System.out.println("Enter method-6");
		System.out.println("Executing method-6 : " + str);
		System.out.println("Exit method-6");
	}

	// Simulate multiple threads processing PAN authorizations
	public static void main(String[] args) {
		PanAuthorizationProcessor processor = new PanAuthorizationProcessor();
		long sleep = 15000;
		// Simulate multiple threads for different authorizations on the same PAN
		new Thread(() -> processor.execute("pan-1", "auth-1", sleep)).start();
		new Thread(() -> processor.execute("pan-1", "auth-2", sleep)).start();
		new Thread(() -> processor.execute("pan-1", "auth-3", sleep)).start();

		// Simulate a thread for a different PAN
		new Thread(() -> processor.execute("pan-2", "auth-1", 45000l)).start();
	}
}
