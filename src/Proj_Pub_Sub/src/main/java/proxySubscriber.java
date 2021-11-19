import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.StringTokenizer;

public class proxySubscriber {
    private ZMQ.Socket subscriber;
    private static int id;
    private ZContext context;

    public proxySubscriber(int idS) {
        id = idS;
        this.context = new ZContext();
        // Socket to talk to server
        this.subscriber = this.context.createSocket(SocketType.XSUB); // or SUB
        System.out.println("Subscriber Connecting to Proxy...");
        this.subscriber.connect("tcp://*:5556");
    }

    // subscribe a topic
    public void subscribe(String topic) {
        // Construct subscribe message: "0x01 topic id"
        String message = "0x01 " + topic + " " + id;
        this.subscriber.send(message.getBytes());

        byte[] response = this.subscriber.recv(); // "Subscribed + topic"
        String[] responseStr = new String(response).split(" ");

        for(int i = 0; i < responseStr.length; i++) {
            System.out.println("Message: " + responseStr[i]);
        }

        if(responseStr[0].equals("Subscribed")) {
            if(responseStr[1].equals(topic)) {
                System.out.println("Client " + id + " subscribed topic " + topic);
            }
        }
        else {
            System.out.println("Client " + id + "failed to subscribe topic " + topic);
        }
    }

    // unsubscribe a topic
    public void unsubscribe(String topic) {
        // Construct unsubscribe message "0x00 topic id"
        String message = "0x00 " + topic + " " + id;
        this.subscriber.send(message.getBytes());

        byte[] response = this.subscriber.recv(); // "Unsubscribed + topic"
        String[] responseStr = new String(response).split(" ");

        if(responseStr[0].equals("Unsubscribed")) {
            if(responseStr[1].equals(topic)) {
                System.out.println("Client " + id + " unsubscribed topic " + topic);
            }
        }
        else {
            System.out.println("Client " + id + " failed to unsubscribe topic " + topic);
        }
    }

    // to consume a message from a topic
    public void get(String topic) {
        // Construct get message "0x02 topic id"
        String message = "0x02 " + topic + " " + id;
        this.subscriber.send(message.getBytes());

        byte[] response = this.subscriber.recv(); // "topic : message"
        String[] responseStr = new String(response).split(" : ");

        if(responseStr[0].equals(topic)) {
            System.out.println("Message for Client " + id + "for topic " + topic + ": " + responseStr[1]);
        }
        else {
            System.out.println("Client" + id + " received message from another topic");
        }

    }

    public static void main(String[] args) {
        proxySubscriber subscriber = new proxySubscriber(1);
        subscriber.subscribe("topicA");
    }
}
