import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.StringTokenizer;

public class proxySubscriber {
    private ZMQ.Socket subscriber;
    private static int id;

    public proxySubscriber(int idS) {
        id = idS;
        try (ZContext context = new ZContext()) {
            // Socket to talk to server
            System.out.println("Collecting updates from weather server");
            this.subscriber = context.createSocket(SocketType.XSUB); // or SUB
            this.subscriber.connect("tcp://*:5556");
        }
    }

    // subscribe a topic
    public void subscribe(String topic) {
        // Construct subscribe message: "0x01 topic id"
        String message = "0x01 " + topic + " " + id;
        this.subscriber.send(message.getBytes(ZMQ.CHARSET));
        System.out.println("Client " + id + " subscribed topic " + topic);
    }

    // unsubscribe a topic
    public void unsubscribe(String topic) {
        // Construct unsubscribe message "0x00 topic id"
        String message = "0x00 " + topic + " " + id;
        this.subscriber.send(message.getBytes(ZMQ.CHARSET));
        System.out.println("Client " + id + " unsubscribed topic " + topic);
    }

    // to consume a message from a topic
    public void get(String topic) {
        // Construct get message "topic + id"
        String message = topic + " " + id;
        this.subscriber.send(message.getBytes(ZMQ.CHARSET));
        byte[] resp = this.subscriber.recv();
        System.out.println("Message for Client " + id + ": " + new String(resp));
    }

    public static void main(String[] args) {

    }
}
