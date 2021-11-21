import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class Proxy {
    private ZMQ.Socket frontend;
    private ZMQ.Socket backend;
    private ZMQ.Socket getSocket;
    private ZMQ.Socket pubConfirmations;
    private ConcurrentHashMap<String, Topic> topics; // key -> topicName; value -> Topic
    private ArrayList<String> topicNames; // array with topic names
    private ScheduledThreadPoolExecutor exec;
    private ZMQ.Poller poller;
    private static ZContext context;

    public Proxy() {
        exec = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(128);

        // Prepare our context and sockets
        context = new ZContext();
        this.frontend = context.createSocket(SocketType.SUB);
        this.frontend.bind("tcp://localhost:5557"); // 5556? conecta-se ao pub ou sub?
        this.frontend.subscribe(ZMQ.SUBSCRIPTION_ALL);

        this.backend = context.createSocket(SocketType.XPUB);
        this.backend.bind("tcp://localhost:5556"); // 5557? conecta-se ao pub ou sub?

        this.getSocket = context.createSocket(SocketType.REP);
        this.getSocket.bind("tcp://localhost:5555");

        this.pubConfirmations = context.createSocket(SocketType.PUSH);
        this.pubConfirmations.bind("tcp://localhost:5558");

        //  Initialize poll set
        this.poller = context.createPoller(2);
        this.poller.register(this.frontend, ZMQ.Poller.POLLIN);
        this.poller.register(this.backend, ZMQ.Poller.POLLIN);
        this.poller.register(this.getSocket, ZMQ.Poller.POLLIN);

        this.topics = new ConcurrentHashMap<>();
        this.topicNames = new ArrayList<>();
    }

    public void run() {
        while(!Thread.currentThread().isInterrupted()) {
            System.out.println("There is currently " + topicNames.size() + " topic(s)");
            System.out.println(topics);
            this.poller.poll();
            if(this.poller.pollin(0)) {
                System.out.println("INSIDE POLLER FRONTEND");
                ZFrame frame = ZFrame.recvFrame(this.frontend);
                byte[] msgData = frame.getData();
                handleFrontend(msgData);
                frame.destroy();
            }

            if(this.poller.pollin(1)) {
                System.out.println("INSIDE POLLER BACKEND");
                ZFrame frame = ZFrame.recvFrame(this.backend);
                byte[] msgData = frame.getData();
                handleBackend(msgData);
                frame.destroy();
            }

            if(this.poller.pollin(2)) {
                System.out.println("INSIDE POLLER GET");
                ZFrame frame = ZFrame.recvFrame(this.getSocket);
                byte[] msgData = frame.getData();
                handleGet(msgData);
                frame.destroy();
            }
        }
    }

    void checkTopics(String topic, int id) {
        for (String t: this.topics.keySet()) {
            if (this.topics.get(t).getName().equals(topic)) {
                if (this.topics.get(t).hasSubscriber(id))
                    this.topics.get(t).removeSubscriber(id);
                if (this.topics.get(t).getSubscribers().isEmpty()) {
                    this.topics.remove(t);
                    this.topicNames.remove(t);
                }
            }
        }

        System.out.println("TOPICS" + this.topics);
        System.out.println("TOPIC NAMES " + this.topicNames);
    }

    public void handleFrontend(byte[] msgData) {
        String msgString = new String(msgData, 0, msgData.length, ZMQ.CHARSET);
        String[] message = msgString.split("//");

        String topic = message[0];
        String messageT = message[1];

        if(this.topicNames.contains(topic)) {
            this.topics.get(topic).addMessage(messageT);
            System.out.println(this.topics.get(topic).getMessages());
        }

        String confirmation = "Message has been successfully published to topic " + topic;
        pubConfirmations.send(confirmation.getBytes(ZMQ.CHARSET));
    }

    public void handleBackend(byte[] msgData) {
        byte b = msgData[0];
        String msgString = new String(msgData, 1, msgData.length - 1, ZMQ.CHARSET);
        String[] message = msgString.split("//");
        System.out.println(Arrays.toString(message));
        String toSend = "";

        // Subscribe message
        if(b == 1) { // "topic//id"
            String topic = message[0];
            int id = Integer.parseInt(message[1]);

            if(this.topicNames.contains(topic)) {
                this.topics.get(topic).addSubscriber(id);
            }
            else {
                Topic newTopic = new Topic(topic);
                newTopic.addSubscriber(id);
                this.topics.put(topic, newTopic);
                this.topicNames.add(topic);
            }
            toSend = "0x01//" + topic + "//" + id + "Topic " + topic + " has been successfully subscribed";

            System.out.println("Subscriber " + id +  " successfully subscribed topic " + topic);

            System.out.println("Topics: " + this.topics);
            System.out.println("Topics: " + this.topicNames);

            backend.send(msgString + "Topic " + topic + " subscribed successfully");

            // return;
        }

        // Unsubscribe message
        else if(b == 0) { // "topic//id"
            String topic = message[0];
            int id = Integer.parseInt(message[1]);

            //this.topics.get(topic).removeSubscriber(id);
            this.checkTopics(topic, id);
            // necessario apagar o topico das listas caso fique sem nenhum subscritor?

            toSend = "0x00//" + topic + "//" + id + "Topic " + topic + " has been successfully unsubscribed";

            System.out.println("Subscriber " + id +  " unsuccessfully subscribed topic " + topic);

            backend.send(msgString + "Topic " + topic + " unsubscribed successfully");
        }

        // Get message
        else if(message[0].equals("0x03")) { // "topic id"
            String topic = message[1];
            int id = Integer.parseInt(message[2]);

            if(this.topicNames.contains(topic)) {
                Topic topicObj = this.topics.get(topic);

                String topicMessage = topicObj.getMessage(id);

                toSend = topic + " : " + topicMessage;
            }
        }

        //System.out.println("TO SEND: " + toSend);
        this.backend.send(toSend.getBytes());
    }

    public void handleGet(byte[] msgData) {
        String msgString = new String(msgData, 0, msgData.length, ZMQ.CHARSET);
        String[] message = msgString.split("//");
        System.out.println(Arrays.toString(message));
        String toSend = "";

        String topic = message[0];
        int id = Integer.parseInt(message[1]);

        if(this.topicNames.contains(topic)) { // toSend começa por 1 se tiver mensagens, 0 se não tiver mais, 2 se nao for subscritor, 3 se o topico nao existir
            Topic t = this.topics.get(topic);
            if(t.hasSubscriber(id)) {
                if(t.checkNext(id)) {
                    String topicMessage = t.getMessage(id);
                    toSend = "1 : " + topic + " : " + topicMessage;
                }
                else {
                    toSend = "0 : " + topic;
                }
            }
            else {
                toSend = "2 : " + topic;
            }
        }
        else {
            toSend = "3 : " + topic;
        }

        this.getSocket.send(toSend.getBytes());
    }

    public static void main(String[] args) {
        Proxy proxy = new Proxy();
        proxy.run();
    }
}
