import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class Proxy {
    private ZMQ.Socket frontend;
    private ZMQ.Socket backend;
    private ConcurrentHashMap<String, Topic> topics; // key -> topicName; value -> Topic
    private ArrayList<String> topicNames; // array with topic names
    private ScheduledThreadPoolExecutor threadExec;
    private ZMQ.Poller poller;
    private ZContext context;

    public Proxy() {
        // Prepare our context and sockets
        this.context = new ZContext();
        this.frontend = context.createSocket(SocketType.SUB);
        this.frontend.bind("tcp://localhost:5557"); // 5556? conecta-se ao pub ou sub?
        this.frontend.subscribe(ZMQ.SUBSCRIPTION_ALL);

        this.backend = context.createSocket(SocketType.XPUB);
        this.backend.bind("tcp://localhost:5556"); // 5557? conecta-se ao pub ou sub?

        //  Initialize poll set
        this.poller = context.createPoller(2);
        this.poller.register(this.frontend, ZMQ.Poller.POLLIN);
        this.poller.register(this.backend, ZMQ.Poller.POLLIN);

        this.topics = new ConcurrentHashMap<>();
        this.topicNames = new ArrayList<>();

        this.threadExec = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(300);

        // Run the proxy until the user interrupts us
        // ZMQ.proxy(this.frontend, this.backend, null);
    }

    public void run() {
        while(!Thread.currentThread().isInterrupted()) {
            System.out.println("Running");
            this.poller.poll();
            if(this.poller.pollin(0)) {
                byte[] message = this.frontend.recv();
                handleFrontend(message);
            }

            if(this.poller.pollin(1)) {
                byte[] message = this.backend.recv();
                handleBackend(message);
            }
        }
    }

    public void handleFrontend(byte[] message) {
        String[] messageStr = new String(message).split(" ");

        for(int i = 0; i < messageStr.length; i++) {
            System.out.println("MessageStr[" + i + "]: " + messageStr[i]);
        }

        if(messageStr[0].equals("0x03")) { //"0x03 topic : message"
            String topic = messageStr[1];
            String messageT = messageStr[3];

            if(this.topicNames.contains(topic)) {
                this.topics.get(topic).addMessage(messageT);
            }
            else {
                Topic newTopic = new Topic(topic);
                newTopic.addMessage(messageT);
                this.topics.put(topic, newTopic);
                this.topicNames.add(topic);
            }

            this.backend.send("Published".getBytes());
        }
    }

    public void handleBackend(byte[] message) {
        System.out.println("BACKKKKKK");
        String[] messageStr = new String(message).split(" ");

        for(int i = 0; i < messageStr.length; i++) {
            System.out.println("MessageStr[" + i + "]: " + messageStr[i]);
        }

        String toSend = "";

        // Subscribe message
        if(messageStr[0].equals("0x01")) { // "0x01 topic id"
            String topic = messageStr[1];
            int id = Integer.parseInt(messageStr[2]);

            if(this.topicNames.contains(topic)) {
                this.topics.get(topic).addSubscriber(id);
            }
            else {
                Topic newTopic = new Topic(topic);
                newTopic.addSubscriber(id);
                this.topics.put(topic, newTopic);
                this.topicNames.add(topic);
            }
            toSend = "Subscribed " + topic;
        }

        // Unsubscribe message
        else if(messageStr[0].equals("0x00")) { // "0x00 topic id"
            String topic = messageStr[1];
            int id = Integer.parseInt(messageStr[2]);

            if(this.topicNames.contains(topic)) {
                this.topics.get(topic).removeSubscriber(id);

                // necessario apagar o topico das listas caso fique sem nenhum subscritor?
            }
            toSend = "Unsubscribed " + topic;
        }

        // Get message
        else if(messageStr[0].equals("0x02")) { // "0x02 topic id"
            String topic = messageStr[1];
            int id = Integer.parseInt(messageStr[2]);

            if(this.topicNames.contains(topic)) {
                Topic topicObj = this.topics.get(topic);

                String topicMessage = topicObj.get_message(id);

                toSend = topic + " : " + topicMessage;
            }
        }

        System.out.println("TO SEND: " + toSend);
        this.backend.send(toSend.getBytes());
    }

    public static void main(String[] args) {
        Proxy proxy = new Proxy();
        proxy.run();
    }
}
