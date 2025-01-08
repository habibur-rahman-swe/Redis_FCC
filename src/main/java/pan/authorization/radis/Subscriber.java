package pan.authorization.radis;

public class Subscriber {

	public static void main(String[] args) {
		PanAuthorizationSystemRedis system = new PanAuthorizationSystemRedis();

		// Start subscriber threads for different PANs
		new Thread(() -> system.subscribe("pan-1")).start();
	}
}
