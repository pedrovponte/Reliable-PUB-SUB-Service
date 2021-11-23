import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;

public class Proxy {
    private ZMQ.Socket frontend;
    private ZMQ.Socket backend;
    private ZMQ.Socket getSocket;
    private ZMQ.Socket pubConfirmations;
    private ZMQ.Socket subChecks;
    private Storage storage;
    private HashMap<Integer, Boolean> connectedSubs;
    private ScheduledThreadPoolExecutor exec;
    private ZMQ.Poller poller;
    private final ZContext context;

    public Proxy() {
        this.exec = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(128);
        this.connectedSubs = new HashMap<>();

        File f = new File("proxy/proxy.ser");
        if(f.exists() && !f.isDirectory()) {
            try {
                FileInputStream fileInput = new FileInputStream("proxy/proxy.ser");
                ObjectInputStream inputObj = new ObjectInputStream(fileInput);
                storage = (Storage) inputObj.readObject();
                inputObj.close();
                fileInput.close();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }

        }
        else {
            this.storage = new Storage();
        }

        // Prepare our context and sockets
        this.context = new ZContext();
        this.frontend = this.context.createSocket(SocketType.SUB);
        this.frontend.bind("tcp://localhost:5557");
        this.frontend.subscribe(ZMQ.SUBSCRIPTION_ALL);

        this.backend = this.context.createSocket(SocketType.XPUB);
        this.backend.bind("tcp://localhost:5556");

        this.getSocket = this.context.createSocket(SocketType.REP);
        this.getSocket.bind("tcp://localhost:5555");

        this.pubConfirmations = this.context.createSocket(SocketType.PUSH);
        this.pubConfirmations.bind("tcp://localhost:5558");

        this.subChecks = this.context.createSocket(SocketType.PULL);
        this.subChecks.bind("tcp://localhost:5559");

        //  Initialize poll set
        this.poller = this.context.createPoller(4);
        this.poller.register(this.frontend, ZMQ.Poller.POLLIN);
        this.poller.register(this.backend, ZMQ.Poller.POLLIN);
        this.poller.register(this.getSocket, ZMQ.Poller.POLLIN);
        this.poller.register(this.subChecks, ZMQ.Poller.POLLIN);
    }

    Runnable serialize = new Runnable() {
        public void run() {
            System.out.println("Serializing...");
            String filename = "proxy/proxy.ser";

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

    Runnable checkSubscribers = new Runnable() {
        public void run() {
            for (Integer id: connectedSubs.keySet()) {
                if (!connectedSubs.get(id)) {
                    System.out.println("Subscriber " + id + " disconnected...");
                    connectedSubs.remove(id);
                }
            }

            if (connectedSubs.isEmpty()) {
                storage.getTopics().clear();
                return;
            }

            for (String t: storage.getTopics().keySet()) {
                for (Integer id: storage.getTopics().get(t).getSubscribers()) {
                    if (!connectedSubs.containsKey(id)) {
                        storage.getTopics().get(t).removeSubscriber(id);
                    }
                }
            }

            connectedSubs.replaceAll((i, v) -> Boolean.FALSE);
        }
    };

    public void run() {
        exec.scheduleAtFixedRate(serialize, 5, 10, TimeUnit.SECONDS);
        exec.scheduleAtFixedRate(checkSubscribers, 0, 5, TimeUnit.SECONDS);

        while(!Thread.currentThread().isInterrupted()) {
            System.out.println("There is/are currently " + storage.getTopicNames().size() + " topic(s)");
            this.poller.poll();
            if(this.poller.pollin(0)) {
                //System.out.println("INSIDE POLLER FRONTEND");
                ZFrame frame = ZFrame.recvFrame(this.frontend);
                byte[] msgData = frame.getData();
                handleFrontend(msgData);
                frame.destroy();
            }

            if(this.poller.pollin(1)) {
                //System.out.println("INSIDE POLLER BACKEND");
                ZFrame frame = ZFrame.recvFrame(this.backend);
                byte[] msgData = frame.getData();
                handleBackend(msgData);
                frame.destroy();
            }

            if(this.poller.pollin(2)) {
                //System.out.println("INSIDE POLLER GET");
                ZFrame frame = ZFrame.recvFrame(this.getSocket);
                byte[] msgData = frame.getData();
                handleGet(msgData);
                frame.destroy();
            }

            if(this.poller.pollin(3)) {
                ZFrame frame = ZFrame.recvFrame(this.subChecks);
                byte[] msgData = frame.getData();
                String subID = new String(msgData, 0, msgData.length, ZMQ.CHARSET);

                connectedSubs.put(Integer.parseInt(subID), Boolean.TRUE);

                //handleFrontend(msgData);
                frame.destroy();
            }
        }
    }

    public int checkTopics(String topic, int id) {
        for (String t: this.storage.getTopics().keySet()) {
            if (this.storage.getTopics().get(t).getName().equals(topic)) {
                if (this.storage.getTopics().get(t).hasSubscriber(id))
                    this.storage.getTopics().get(t).removeSubscriber(id);
                else {
                    System.out.println("Subscriber " + id + " isn't subscribed to this topic.");
                    return -1;
                }
                if (this.storage.getTopics().get(t).getSubscribers().isEmpty()) {
                    this.storage.getTopics().remove(t);
                    this.storage.getTopicNames().remove(t);
                }
                return 0;
            }
        }
        System.out.println("Subscriber " + id + " isn't subscribed to this topic.");
        return -1;
    }

    public void handleFrontend(byte[] msgData) {
        String msgString = new String(msgData, 0, msgData.length, ZMQ.CHARSET);
        String[] message = msgString.split("//");

        String topic = message[0];
        String messageT = message[1];
        String confirmation = "";

        if(this.storage.getTopicNames().contains(topic)) {
            this.storage.getTopics().get(topic).addMessage(messageT);
            System.out.println("Message added succesfully to topic " + topic);
            confirmation = "Message has been successfully published to topic " + topic;
        }
        else {
            confirmation = "No one has subscribed to this topic yet. Message discarded.";
        }

        this.pubConfirmations.send(confirmation.getBytes(ZMQ.CHARSET));
    }

    public void handleBackend(byte[] msgData) {
        byte b = msgData[0];
        String msgString = new String(msgData, 1, msgData.length - 1, ZMQ.CHARSET);
        String[] message = msgString.split("//");

        // Subscribe message
        if(b == 1) { // "topic//id"
            String topic = message[0];
            int id = Integer.parseInt(message[1]);

            if(this.storage.getTopicNames().contains(topic)) {
                this.storage.getTopics().get(topic).addSubscriber(id);
            }
            else {
                Topic newTopic = new Topic(topic);
                newTopic.addSubscriber(id);
                this.storage.getTopics().put(topic, newTopic);
                this.storage.getTopicNames().add(topic);
            }

            System.out.println("Subscriber " + id +  " successfully subscribed topic " + topic);

            this.backend.send(msgString + "Topic " + topic + " successfully subscribed.");

        }

        // Unsubscribe message
        else if(b == 0) { // "topic//id"
            String topic = message[0];
            int id = Integer.parseInt(message[1]);

            //this.topics.get(topic).removeSubscriber(id);
            if (this.checkTopics(topic, id) < 0) {
                this.backend.send(msgString + "Subscriber " + id + " isn't subscribed to topic " + topic);
                return;
            }

            System.out.println("Subscriber " + id +  " successfully unsubscribed topic " + topic);

            this.backend.send(msgString + "Topic " + topic + " successfully unsubscribed.");
        }

        else {
            System.out.println("Invalid message");
        }
    }

    public void handleGet(byte[] msgData) {
        String msgString = new String(msgData, 0, msgData.length, ZMQ.CHARSET);
        String[] message = msgString.split("//");
        String toSend = "";

        String topic = message[0];
        int id = Integer.parseInt(message[1]);

        if(this.storage.getTopicNames().contains(topic)) { // toSend starts by 1 if has new messages, 0 if not, 2 if not subscriber, 3 if topic doesn't exist
            Topic t = this.storage.getTopics().get(topic);
            if(t.hasSubscriber(id)) {
                if(t.checkNext(id)) {
                    String topicMessage = t.getMessage(id);
                    toSend = "1 : " + topic + " : " + topicMessage;
                }
                else { // no new messages
                    toSend = "0 : " + topic;
                }
            }
            else { // topic not subscribed
                toSend = "2 : " + topic;
            }
        }
        else { // topic not exist
            toSend = "3 : " + topic;
        }

        this.getSocket.send(toSend.getBytes());
    }

    public static void main(String[] args) {
        Proxy proxy = new Proxy();
        proxy.run();
    }
}
