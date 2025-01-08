package pan.authorization.syste;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PanAuthorizationSystem {

    // Map to store PAN-specific queues
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> panQueues = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        PanAuthorizationSystem system = new PanAuthorizationSystem();

        // Simulate multiple authorization requests for the same PAN
        new Thread(() -> system.addAuthorizationRequest("pan-1", "auth-1")).start();
        new Thread(() -> system.addAuthorizationRequest("pan-1", "auth-2")).start();
        new Thread(() -> system.addAuthorizationRequest("pan-1", "auth-3")).start();
        new Thread(() -> system.addAuthorizationRequest("pan-1", "auth-4")).start();

        // Start processing requests for pan-1
        new Thread(() -> system.processRequests("pan-1")).start();
    }

    // Method to add an authorization request to a PAN-specific queue
    public void addAuthorizationRequest(String pan, String auth) {
        panQueues.computeIfAbsent(pan, key -> new ConcurrentLinkedQueue<>()).add(auth);
        System.out.println("Added authorization: " + auth + " to PAN: " + pan);
    }

    // Method to process authorization requests for a specific PAN
    public void processRequests(String pan) {
        while (true) {
            ConcurrentLinkedQueue<String> queue = panQueues.get(pan);
            if (queue != null && !queue.isEmpty()) {
                synchronized (queue) { // Ensure only one thread processes the queue at a time
                    String auth = queue.peek(); // Get the first authorization
                    if (auth != null) {
                        System.out.println("Processing authorization: " + auth + " for PAN: " + pan);
                        try {
                            Thread.sleep(2000); // Simulate authorization processing
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        queue.poll(); // Remove the processed authorization
                        System.out.println("Completed authorization: " + auth + " for PAN: " + pan);
                    }
                }
            } else {
                try {
                    Thread.sleep(1000); // Wait before checking again
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}