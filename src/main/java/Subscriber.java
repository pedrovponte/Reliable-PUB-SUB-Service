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
    private ZMQ.Socket getSocket;
    private static int id;
    private final ZContext context;

    public Subscriber(int idS) {
        id = idS;
        this.context = new ZContext();
        this.subscriber = this.context.createSocket(SocketType.SUB);
        this.getSocket = this.context.createSocket(SocketType.REQ);
        System.out.println("Subscriber Connecting to Proxy...");
        this.subscriber.connect("tcp://*:5556");
        this.getSocket.connect("tcp://*:5555");
    }

    // subscribe a topic
    public void subscribe(String topic) {
        System.out.println("Subscribing " + topic + "...");
        // Construct subscribe message: "topic//id"
        String message = topic + "//" + id;

        if(!this.subscriber.subscribe(message.getBytes(ZMQ.CHARSET))) {
            System.out.println("Failed to subscribe topic '" + topic + "'.");
            return;
        }

        String response = this.subscriber.recvStr();

        if(response == null) {
            return;
        }

        System.out.println(response.split(message)[1]);
    }

    // unsubscribe a topic
    public void unsubscribe(String topic) {
        // Construct subscribe message: "topic//id"
        String message = topic + "//" + id;

        if(!this.subscriber.unsubscribe(message.getBytes())) {
            System.out.println("Failed to unsubscribe topic '" + topic + "'.");
            return;
        }

        String response = this.subscriber.recvStr();

        if(response == null) {
            System.out.println("Failed to receive unsubscribe confirmation.");
            return;
        }

        System.out.println(response.split(message)[1]);
    }

    // to consume a message from a topic
    public void get(String topic) {
        // Construct get message "topic id"
        String message = topic + "//" + id;

        if(!this.getSocket.send(message.getBytes())) {
            System.out.println("Failed to send get message to proxy for topic '" + topic + "'.");
            return;
        }

        byte[] response = this.getSocket.recv(0); // "topic : message"

        if(response == null) {
            System.out.println("Failed to receive a message fot topic '" + topic + "'.");
            return;
        }

        String[] responseStr = new String(response).split(" : ");

        System.out.println(Arrays.toString(responseStr));

        if(responseStr[1].equals(topic)) { // response começa por 1 se tiver mensagens, 0 se não tiver mais, 2 se nao for subscritor, 3 se o topico nao existir
            switch (responseStr[0]) {
                case "0":
                    System.out.println("Client " + id + " has no new messages to receive on topic '" + topic + "'.");
                    break;
                case "1":
                    System.out.println("Message for Client " + id + " for topic '" + topic + "': " + responseStr[2]);
                    break;
                case "2":
                    System.out.println("Client " + id + " didn't subscribe the topic '" + topic + "'.");
                    break;
                case "3":
                    System.out.println("Client " + id + " asked for a topic ('" + topic + "') that doesn't exist.");
                    break;
            }
        }
        else {
            System.out.println("Client " + id + " received a message for a different topic that didn't ask.");
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