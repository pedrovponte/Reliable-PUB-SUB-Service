import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class proxyPublisher {
    public static void main(String[] args) throws Exception {
        // Prepare our context and publisher for
        try (ZContext context = new ZContext()) {
            ZMQ.Socket publisher = context.createSocket(SocketType.PUB);
            System.out.println("Publisher Connecting to Proxy...");
            publisher.bind("tcp://*:5557");

            while (!Thread.currentThread().isInterrupted()) {
                // Send message to all subscribers
                String update = String.format(args[0] + " Welcome to my publications!!");
                publisher.send(update, 0);

                Thread.sleep(100);
            }
        }
    }
}
