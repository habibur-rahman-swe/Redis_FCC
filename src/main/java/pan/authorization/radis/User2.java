package pan.authorization.radis;

public class User2 {
	public static void main(String[] args) {
		PanAuthorizationSystemRedis system = new PanAuthorizationSystemRedis();

		// Simulate adding authorization requests
		new Thread(() -> system.addAuthorizationRequest("pan-1", "auth-4")).start();
		new Thread(() -> system.addAuthorizationRequest("pan-1", "auth-5")).start();
		new Thread(() -> system.addAuthorizationRequest("pan-1", "auth-6")).start();
	}
}
