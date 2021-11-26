import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;

import java.io.*;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Proxy {
    private ZMQ.Socket frontend;
    private ZMQ.Socket backend;
    private ZMQ.Socket getSocket;
    private ZMQ.Socket confirmGetSocket;
    private ZMQ.Socket pubRequests;
    private ZMQ.Socket subRequests;
    private Storage storage;
    private boolean unsubFlag = false;
    private int putFlag = -1;
    private int unsubID = -1;
    private ScheduledThreadPoolExecutor exec;
    private ZMQ.Poller poller;
    private final ZContext context;

    public Proxy() {
        this.exec = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(128);

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

        this.pubRequests = this.context.createSocket(SocketType.REP);
        this.pubRequests.bind("tcp://localhost:5558");

        this.subRequests = this.context.createSocket(SocketType.REP);
        this.subRequests.bind("tcp://localhost:5559");

        this.confirmGetSocket = this.context.createSocket(SocketType.PULL);
        this.confirmGetSocket.bind("tcp://localhost:5554");


        //  Initialize poll set
        this.poller = this.context.createPoller(5);
        this.poller.register(this.frontend, ZMQ.Poller.POLLIN);
        this.poller.register(this.backend, ZMQ.Poller.POLLIN);
        this.poller.register(this.getSocket, ZMQ.Poller.POLLIN);
        this.poller.register(this.subRequests, ZMQ.Poller.POLLIN);
        this.poller.register(this.pubRequests, ZMQ.Poller.POLLIN);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                // System.out.println("Running Shutdown Hook");
                frontend.close();
                backend.close();
                getSocket.close();
                subRequests.close();
                pubRequests.close();
                confirmGetSocket.close();
            }
        });
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

    Runnable state = new Runnable() {
        public void run() {
            System.out.println("\nNUMBER OF TOPICS: " + storage.getTopicNames().size());
            System.out.println("TOPICS AND SUBSCRIBERS:");
            for (String t : storage.getTopics().keySet()) {
                String ids = "";
                for (int id : storage.getTopics().get(t).getSubscribers()) {
                    ids += id + " ";
                }
                System.out.println(storage.getTopics().get(t).getName() + " -> [ " + ids + "]");
            }
        }
    };

    public void run() {
        exec.scheduleAtFixedRate(serialize, 5, 10, TimeUnit.SECONDS);
        exec.scheduleAtFixedRate(state, 2, 5, TimeUnit.SECONDS);

        while(!Thread.currentThread().isInterrupted()) {
            this.poller.poll();
            if(this.poller.pollin(0)) {
                ZFrame frame = ZFrame.recvFrame(this.frontend);
                byte[] msgData = frame.getData();
                handleFrontend(msgData);
                frame.destroy();
            }

            if(this.poller.pollin(1)) {
                ZFrame frame = ZFrame.recvFrame(this.backend);
                byte[] msgData = frame.getData();
                handleBackend(msgData);
                frame.destroy();
            }

            if(this.poller.pollin(2)) {
                ZFrame frame = ZFrame.recvFrame(this.getSocket);
                byte[] msgData = frame.getData();
                handleGet(msgData);
                frame.destroy();
            }

            if(this.poller.pollin(3)) {
                String request = this.subRequests.recvStr(0);
                String[] args = request.split("//");

                switch(args[0]) {
                    case "REQ_UNSUB":
                        unsubID = Integer.parseInt(args[1]);
                        unsubFlag = true;
                        this.subRequests.send(("REP_UNSUB//" + unsubID).getBytes(ZMQ.CHARSET));
                        break;
                    case "REQ_SUBS":
                        int subID = Integer.parseInt(args[1]);
                        String topics = "";
                        List<String> subscribedTopics = storage.getTopicsSubscribedBySubscriber(subID);

                        for (String topic: subscribedTopics) {
                            topics += topic;
                            if (subscribedTopics.indexOf(topic) != subscribedTopics.size() - 1)
                                topics += "//";
                        }

                        this.subRequests.send(("REP_SUBS//" + topics).getBytes(ZMQ.CHARSET));
                        break;
                }
            }

            if(this.poller.pollin(4)) {
                this.pubRequests.recvStr(0);
                if (putFlag == 1)
                    this.pubRequests.send("PUT_ACK_SUCC".getBytes(ZMQ.CHARSET));
                else if (putFlag == 0)
                    this.pubRequests.send("PUT_ACK_DENY".getBytes(ZMQ.CHARSET));
                else
                    this.pubRequests.send("PUT_NACK".getBytes(ZMQ.CHARSET));
                putFlag = -1;
            }
        }
    }

    public void checkTopics(String topic, int id) {
        for (String t: this.storage.getTopics().keySet()) {
            if (this.storage.getTopics().get(t).getName().equals(topic)) {
                if (this.storage.getTopics().get(t).hasSubscriber(id))
                    this.storage.getTopics().get(t).removeSubscriber(id);
                else {
                    System.out.println("Subscriber " + id + " isn't subscribed to this topic.");
                    return;
                }
                if (this.storage.getTopics().get(t).getSubscribers().isEmpty()) {
                    this.storage.getTopics().remove(t);
                    this.storage.getTopicNames().remove(t);
                }
                return;
            }
        }
        System.out.println("Subscriber " + id + " isn't subscribed to this topic.");
    }

    public void handleFrontend(byte[] msgData) {
        String msgString = new String(msgData, 0, msgData.length, ZMQ.CHARSET);
        String[] message = msgString.split("//");

        String topic = message[0];
        String messageT = message[1];

        if(this.storage.getTopicNames().contains(topic)) {
            this.storage.getTopics().get(topic).addMessage(messageT);
            System.out.println("Message added successfully to topic: " + topic);
            putFlag = 1;
        }
        else putFlag = 0;
    }

    public void handleBackend(byte[] msgData) {
        byte b = msgData[0];
        String msgString = new String(msgData, 1, msgData.length - 1, ZMQ.CHARSET);
        String[] message = msgString.split("//");
        String toSend = "";

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

            System.out.println("Subscriber " + id +  " successfully subscribed topic: " + topic);

            this.backend.send(msgString + "Topic " + topic + " successfully subscribed.");
        }

        // Unsubscribe message
        else if(b == 0) { // "topic//id"
            String topic = message[0];
            int id = Integer.parseInt(message[1]);

            if (unsubFlag && unsubID == id) {
                System.out.println("Subscriber " + id + " successfully unsubscribed topic: " + topic);
                this.checkTopics(topic, id);
                unsubFlag = false;
                unsubID = -1;
            }
        }
    }

    public void handleGet(byte[] msgData) {
        String msgString = new String(msgData, 0, msgData.length, ZMQ.CHARSET);
        String[] message = msgString.split("//");
        String toSend = "";

        String topic = message[0];
        int id = Integer.parseInt(message[1]);

        boolean hasMessage = false;

        if(this.storage.getTopicNames().contains(topic)) {
            Topic t = this.storage.getTopics().get(topic);
            if (t.getMessages().isEmpty()) {
                toSend = "GET_EMPTY : " + topic;
            }
            else if(t.hasSubscriber(id)) {
                if(t.checkNext(id)) {
                    String topicMessage = t.getMessage(id);
                    toSend = "GET_SUCC : " + topic + " : " + topicMessage;
                    hasMessage = true;
                }
                else { // no new messages
                    toSend = "GET_NONEW : " + topic;
                }
            }
            else { // topic not subscribed
                toSend = "GET_UNSUB : " + topic;
            }
        }
        else { // topic not exist
            toSend = "GET_UNEXIST : " + topic;
        }

        this.getSocket.send(toSend.getBytes());

        this.confirmGetSocket.setReceiveTimeOut(5000);
        String request = this.confirmGetSocket.recvStr(0);

        if (request == null) {
            System.out.println("Get Protocol unsuccessful.");
            return;
        }

        String[] args = request.split("//");

        if (args[0].equals("ACK_GET") && hasMessage)
            this.storage.getTopics().get(topic).updateMessagesForSubscriber(id);
    }

    public static void main(String[] args) throws RemoteException {
        Proxy proxy = new Proxy();
        proxy.run();
    }
}
