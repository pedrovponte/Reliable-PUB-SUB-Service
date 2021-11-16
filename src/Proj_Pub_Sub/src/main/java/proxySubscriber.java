import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.StringTokenizer;

public class proxySubscriber {
    public static void main(String[] args) {
        try (ZContext context = new ZContext()) {
            // Socket to talk to server
            System.out.println("Collecting updates from weather server");
            ZMQ.Socket subscriber = context.createSocket(SocketType.SUB);
            subscriber.connect("tcp://*:5556");

            subscriber.subscribe(args[0].getBytes());

            int total_messages = 0;

            while (total_messages < 100) {
                // Use trim to remove the tailing '0' character
                String string = subscriber.recvStr(0).trim();

                System.out.println("Message: " + string);
                total_messages++;
            }
        }
    }
}
