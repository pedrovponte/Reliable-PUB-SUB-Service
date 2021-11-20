import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.StringTokenizer;

public class Subscriber implements SubscriberInterface {
    private ZMQ.Socket subscriber;
    private static int id;
    private final ZContext context;

    public Subscriber(int idS) {
        id = idS;
        this.context = new ZContext();
        subscriber = context.createSocket(SocketType.SUB);
        System.out.println("Subscriber Connecting to Proxy...");
        subscriber.connect("tcp://*:5556");
    }

    // subscribe a topic
    public void subscribe(String topic) {
        // Construct subscribe message: "0x01//topic//id"
        String message = "0x01//" + topic + "//" + id;
        this.subscriber.subscribe(message.getBytes());

        String response = this.subscriber.recvStr();
        System.out.println(response.split(message)[1]);

        /*for(int i = 0; i < responseStr.length; i++) {
            System.out.println("Message: " + responseStr[i]);
        }

        if(responseStr[0].equals("Subscribed")) {
            if(responseStr[1].equals(topic)) {
                System.out.println("Client " + id + " subscribed topic " + topic);
            }
        }
        else {
            System.out.println("Client " + id + "failed to subscribe topic " + topic);
        }*/
    }

    // unsubscribe a topic
    public void unsubscribe(String topic) {
        // Construct unsubscribe message "0x00//topic//id"
        String message = "0x00//" + topic + "//" + id;
        System.out.println("MESSAGE: " + message);
        this.subscriber.unsubscribe(message.getBytes());

        String response = this.subscriber.recvStr();
        System.out.println(response.split(message)[1]);

//        if(responseStr[0].equals("Unsubscribed")) {
//            if(responseStr[1].equals(topic)) {
//                System.out.println("Client " + id + " unsubscribed topic " + topic);
//            }
//        }
//        else {
//            System.out.println("Client " + id + " failed to unsubscribe topic " + topic);
//        }
    }

    // to consume a message from a topic
    public void get(String topic) {
        // Construct get message "0x03 topic id"
        String message = "0x03//" + topic + "//" + id;
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

    public static void main(String[] args) throws RemoteException {
        if (args.length < 1) System.out.println("Please specify an (unique) ID for this subscriber");
        try {
            Subscriber obj = new Subscriber(Integer.parseInt(args[0])); // user passa port a conectar?
            SubscriberInterface stub = (SubscriberInterface) UnicastRemoteObject.exportObject(obj, 0);
            Registry rmiRegistry = LocateRegistry.createRegistry(1099);
            rmiRegistry.rebind("Sub" + args[0], stub);
            System.out.printf("Subscriber %s ready\n", args[0]);
        } catch (ExportException f) {
            Subscriber obj = new Subscriber(Integer.parseInt(args[0]));
            SubscriberInterface stub = (SubscriberInterface) UnicastRemoteObject.exportObject(obj, 0);
            Registry rmiRegistry = LocateRegistry.getRegistry(1099);
            rmiRegistry.rebind("Sub" + args[0], stub);
            System.out.printf("Subscriber %s ready\n", args[0]);
        }
        catch (RemoteException e) {
            e.printStackTrace();
        }
    }
}