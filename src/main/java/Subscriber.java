import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.*;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Subscriber implements SubscriberInterface {
    private ZMQ.Socket subscriber;
    private ZMQ.Socket getSocket;
    private static ZMQ.Socket notifySocket;
    private static int id;
    private final ZContext context;
    private static StorageSub storage;

    public Subscriber(int idS) {
        id = idS;

        File f = new File("subscribers/subscriber_" + id + ".ser");
        if(f.exists() && !f.isDirectory()) {
            try {
                FileInputStream fileInput = new FileInputStream("subscribers/subscriber_" + id + ".ser");
                ObjectInputStream inputObj = new ObjectInputStream(fileInput);
                storage = (StorageSub) inputObj.readObject();
                inputObj.close();
                fileInput.close();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }

        }
        else {
            storage = new StorageSub();
        }

        this.context = new ZContext();
        this.subscriber = this.context.createSocket(SocketType.SUB);
        this.getSocket = this.context.createSocket(SocketType.REQ);
        notifySocket = this.context.createSocket(SocketType.PUSH);
        this.subscriber.connect("tcp://*:5556");
        this.getSocket.connect("tcp://*:5555");
        notifySocket.connect("tcp://*:5559");
        ScheduledThreadPoolExecutor exec = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(128);
        exec.scheduleAtFixedRate(Subscriber::notifyProxy, 0, 2, TimeUnit.SECONDS);
        exec.scheduleAtFixedRate(checkTime, 5, 1, TimeUnit.MINUTES);
        exec.scheduleAtFixedRate(serialize, 5, 10, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                // System.out.println("Running Shutdown Hook");
                exec.execute(serialize);
                subscriber.close();
                getSocket.close();
                notifySocket.close();
            }
        });
    }


    Runnable checkTime = new Runnable() {
        @Override
        public void run() {
            ConcurrentHashMap<String, String> times = storage.getTopicsSubscribed();
            // System.out.println("Checking times...");

            for(String t : times.keySet()) {
                String time = times.get(t);

                if(System.currentTimeMillis() - Long.parseLong(time) > 300000) { // 5 minutes -> 300000
                    unsubscribe(t);
                }
            }
        }
    };

    Runnable serialize = new Runnable() {
        public void run() {
            System.out.println("Serializing...");
            String filename = "subscribers/subscriber_" + id + ".ser";

            File file = new File(filename);
            if (!file.exists()) {
                file.getParentFile().mkdirs();
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            try {
                FileOutputStream fileStream = new FileOutputStream(filename);
                ObjectOutputStream outputStream = new ObjectOutputStream(fileStream);
                outputStream.writeObject(storage);
                outputStream.close();
                fileStream.close();
            } catch (IOException e ) {
                e.printStackTrace();
            }

        }
    };

    public static void notifyProxy() {
        if (storage.getTopicsSubscribed().isEmpty())
            return;
        System.out.println("Notifying Proxy...");
        String msg = String.valueOf(id);
        notifySocket.send(msg.getBytes());
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

        subscriber.setReceiveTimeOut(5000);
        String response = this.subscriber.recvStr();

        if(response == null) {
            System.out.println("Failed to receive subscribe confirmation.");
            return;
        }

        System.out.println("Topic " + topic + " successfully subscribed.");
        storage.addTopic(topic, String.valueOf(System.currentTimeMillis()));
    }

    // unsubscribe a topic
    public void unsubscribe(String topic) {
        // Construct subscribe message: "topic//id"
        String message = topic + "//" + id;

        System.out.println("Unsubscribing topic " + topic + "...");

        if (!storage.getTopics().contains(topic)) {
            System.out.println("Subscriber " + id + " not subscribed to topic " + topic + ".");
            return;
        }

        if(!this.subscriber.unsubscribe(message.getBytes())) {
            System.out.println("Failed to unsubscribe topic '" + topic + "'.");
            return;
        }

        /*String response = this.subscriber.recvStr();

        if(response == null) {
            System.out.println("Failed to receive unsubscribe confirmation.");
            return;
        }*/

        System.out.println("Topic " + topic + " unsubscribed successfully.");
        storage.removeTopic(topic);
    }

    // to consume a message from a topic
    public void get(String topic) {
        // Construct get message "topic id"
        String message = topic + "//" + id;

        if(!this.getSocket.send(message.getBytes())) {
            System.out.println("Failed to send get message to proxy for topic '" + topic + "'.");
            return;
        }

        subscriber.setReceiveTimeOut(5000);
        byte[] response = this.getSocket.recv(0); // "topic : message"

        if(response == null) {
            System.out.println("Failed to receive a message for topic '" + topic + "'.");
            return;
        }

        String[] responseStr = new String(response).split(" : ");

        if(responseStr[1].equals(topic)) { // response começa por 1 se tiver mensagens, 0 se não tiver mais, 2 se nao for subscritor, 3 se o topico nao existir
            switch (responseStr[0]) {
                case "0":
                    System.out.println("Client " + id + " has no new messages to receive on topic '" + topic + "'.");
                    storage.replaceTimeTopic(topic, String.valueOf(System.currentTimeMillis()));
                    break;
                case "1":
                    System.out.println("Message for Client " + id + " for topic '" + topic + "': " + responseStr[2]);
                    storage.replaceTimeTopic(topic, String.valueOf(System.currentTimeMillis()));
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