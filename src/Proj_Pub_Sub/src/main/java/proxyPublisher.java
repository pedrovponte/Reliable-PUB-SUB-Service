import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class proxyPublisher {
    private ZMQ.Socket publisher;

    public proxyPublisher() {
        try (ZContext context = new ZContext()) {
            this.publisher = context.createSocket(SocketType.XPUB); // or PUB?
            System.out.println("Publisher Connecting to Proxy...");
            this.publisher.connect("tcp://*:5557");
        }
    }

    public void put(String topic, String message) {
        // Send message in format "topic : message"
        String to_send = topic + " : " + message;
        this.publisher.send(to_send);

        byte[] confirmation = this.publisher.recv();
        String confirmation_str = new String(confirmation);

        if(!confirmation_str.equals("Published")) {
            System.out.println("Message not received by proxy");
        }
    }





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
