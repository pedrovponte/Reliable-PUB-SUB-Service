import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;

public class Publisher implements PublisherInterface {
    private ZMQ.Socket publisher;
    private final ZContext context;

    public Publisher(int idP) {
        context = new ZContext();
        this.publisher = context.createSocket(SocketType.XPUB); // or PUB?
        System.out.println("Publisher Connecting to Proxy...");
        this.publisher.connect("tcp://*:5557");
    }

    public void put(String topic, String message) {
        // Send message in format "0x02//topic//message"
        String to_send = topic + "//" + message;
        this.publisher.send(to_send.getBytes());
        System.out.println("Message Sent: " + to_send);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) System.out.println("Please specify an (unique) ID for this publisher");
        try {
            Publisher obj = new Publisher(Integer.parseInt(args[0])); // user passa port a conectar?
            PublisherInterface stub = (PublisherInterface) UnicastRemoteObject.exportObject(obj, 0);
            Registry rmiRegistry = LocateRegistry.createRegistry(1099);
            rmiRegistry.rebind("Pub" + args[0], stub);
            System.out.printf("Publisher %s ready\n", args[0]);
        } catch (ExportException f) {
            Publisher obj = new Publisher(Integer.parseInt(args[0]));
            PublisherInterface stub = (PublisherInterface) UnicastRemoteObject.exportObject(obj, 0);
            Registry rmiRegistry = LocateRegistry.getRegistry(1099);
            rmiRegistry.rebind("Pub" + args[0], stub);
            System.out.printf("Publisher %s ready\n", args[0]);
        }
        catch (RemoteException e) {
            e.printStackTrace();
        }
    }
}