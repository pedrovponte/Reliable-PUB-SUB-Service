import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class pubSubProxy {
    private ZMQ.Socket frontend;
    private ZMQ.Socket backend;
    private ConcurrentHashMap<String, ArrayList<String>> topicsMessages;
    private ArrayList<String> topics;
    private ScheduledThreadPoolExecutor threadExec;
    private ZMQ.Poller poller;

    public pubSubProxy() {
        // Prepare our context and sockets
        try (ZContext context = new ZContext()){
            this.frontend = context.createSocket(SocketType.XSUB);
            this.frontend.bind("tcp://localhost:5557");

            this.backend = context.createSocket(SocketType.XPUB);
            this.backend.bind("tcp://localhost:5556");

            //  Initialize poll set
            this.poller = context.createPoller(2);
            this.poller.register(this.frontend, ZMQ.Poller.POLLIN);
            this.poller.register(this.backend, ZMQ.Poller.POLLIN);

            this.topicsMessages = new ConcurrentHashMap<>();
            this.topics = Collections.list(this.topicsMessages.keys());

            this.threadExec = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(300);

            // Run the proxy until the user interrupts us
            //ZMQ.proxy(this.frontend, this.backend, null);
        }
    }

    public void run() {
        while(!Thread.currentThread().isInterrupted()) {
            this.poller.poll();

            if(this.poller.pollin(0)) {
                byte[] message = this.frontend.recv();
                handle_frontend(message);
            }

            if(this.poller.pollin(1)) {
                byte[] message = this.backend.recv();
                handle_backend(message);
            }
        }
    }

    public void handle_frontend(byte[] message) {
        String[] messageStr = new String(message).split(" ");

        if(messageStr[0].equals("0x01")) { // "0x01 topic id"
            String topic = messageStr[1];
            int id = Integer.getInteger(messageStr[2]);

            if(this.topics.contains(topic)) {

            }
            else {

            }
        }
        else if(messageStr[0].equals("0x00")) { // "0x00 topic id"
            String topic = messageStr[1];
            int id = Integer.getInteger(messageStr[2]);

            if(this.topics.contains(topic)) {
                
            }
        }
    }

    public void handle_backend(byte[] message) {

    }

    public static void main(String[] args) {

    }
}
