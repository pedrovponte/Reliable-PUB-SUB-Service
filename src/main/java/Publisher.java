import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;

public class Publisher implements PublisherInterface {
    private ZMQ.Socket publisher;
    private ZContext context;
    private static int id;
    private ZMQ.Socket confirmations;

    public Publisher(int idP) {
        context = new ZContext();
        this.publisher = context.createSocket(SocketType.XPUB); // or PUB?
        this.publisher.connect("tcp://*:5557");
        id = idP;

        this.confirmations = context.createSocket(SocketType.REQ);
        this.confirmations.connect("tcp://localhost:5558");

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                // System.out.println("Running Shutdown Hook");
                publisher.close();
                confirmations.close();
            }
        });
    }

    public void put(String topic, String message) {
        // Send message in format "topic//message"
        String to_send = topic + "//" + message;

        if(!this.publisher.send(to_send.getBytes())) {
            System.out.println("Publisher failed to put a message on proxy.");
            return;
        }

        this.confirmations.send(("PUT_" + topic).getBytes(ZMQ.CHARSET));

        String reply = this.confirmations.recvStr(0);

        switch(reply) {
            case "PUT_ACK_SUCC":
                System.out.println("Message added successfully to topic: " + topic);
                break;
            case "PUT_ACK_DENY":
                System.out.println("There is no one subscribed to topic: " + topic + ". Message discarded");
                break;
            case "PUT_NACK":
                System.out.println("Failed to add message to topic: " + topic);
                break;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) System.out.println("Please specify an (unique) ID for this publisher");
        try {
            Registry rmiRegistry = LocateRegistry.createRegistry(1099);
            Publisher obj = new Publisher(Integer.parseInt(args[0])); // user passa port a conectar?
            PublisherInterface stub = (PublisherInterface) UnicastRemoteObject.exportObject(obj, 0);
            rmiRegistry.rebind("Pub" + args[0], stub);
            System.out.printf("Publisher %s ready\n", args[0]);
        } catch (ExportException f) {
            Registry rmiRegistry = LocateRegistry.getRegistry(1099);
            Publisher obj = new Publisher(Integer.parseInt(args[0]));
            PublisherInterface stub = (PublisherInterface) UnicastRemoteObject.exportObject(obj, 0);
            rmiRegistry.rebind("Pub" + args[0], stub);
            System.out.printf("Publisher %s ready\n", args[0]);
        }
        catch (RemoteException e) {
            e.printStackTrace();
        }
    }
}