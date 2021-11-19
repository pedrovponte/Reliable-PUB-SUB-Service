import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class proxyPublisher {
    private ZMQ.Socket publisher;
    private ZContext context;

    public proxyPublisher() {
        this.context = new ZContext();
        this.publisher = this.context.createSocket(SocketType.PUB); // or PUB?
        System.out.println("Publisher Connecting to Proxy...");
        this.publisher.connect("tcp://*:5557");
    }

    public void put(String topic, String message) {
        // Send message in format "0x03 topic : message"
        String to_send = "0x03 " + topic + " : " + message;
        this.publisher.send(to_send.getBytes());
        System.out.println("Message Sent: " + to_send);

        byte[] confirmation = this.publisher.recv();
        String confirmation_str = new String(confirmation);

        if(!confirmation_str.equals("Published")) {
            System.out.println("Message not received by proxy");
            return;
        }

        System.out.println("Message saved by proxy");
    }





    public static void main(String[] args) throws Exception {
        proxyPublisher publisher = new proxyPublisher();
        publisher.put("TopicA", "blablabla");
    }
}
