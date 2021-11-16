import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class pubSubProxy {
    public static void main(String[] args) {
        // Prepare our context and sockets
        try (ZContext context = new ZContext()){
            ZMQ.Socket frontend = context.createSocket(SocketType.XSUB);
            frontend.connect("tcp://localhost:5557");

            ZMQ.Socket backend = context.createSocket(SocketType.XPUB);
            backend.connect("tcp://localhost:5556");

            // Run the proxy until the user interrupts us
            ZMQ.proxy(frontend, backend, null);
        }
    }
}
