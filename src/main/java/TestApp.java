import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;

public class TestApp {

    public static void main(String[] args) throws RemoteException, NotBoundException {
        if (args.length < 2)
            throw new IllegalArgumentException("Not enough arguments");

        Registry reg = LocateRegistry.getRegistry("localhost");
        String id = args[1];

        PublisherInterface publisher;
        SubscriberInterface subscriber;
        int index = 2;
        StringBuilder topic = new StringBuilder();
        StringBuilder msg = new StringBuilder();

        switch (args[0].toLowerCase()) {
            case "put":
                // put id [topic] [message]
                for (int x = 2; x < args.length; x++) {
                    index++;
                    if (args[x].contains("[")) {
                        if (args[x].contains("]")) {
                            topic.append(args[x], 1, args[x].length()-1);
                            break;
                        }
                        topic.append(args[x].substring(1));
                    }
                    else if (args[x].contains("]")) {
                        topic.append(args[x], 0, args[x].length() - 1);
                        break;
                    }
                    else
                        topic.append(args[x]);
                    topic.append(" ");
                }

                for (int x = index; x < args.length; x++) {
                    if (args[x].contains("[")) {
                        if (args[x].contains("]")) {
                            msg.append(args[x], 1, args[x].length()-1);
                            break;
                        }
                        msg.append(args[x].substring(1));
                    }
                    else if (args[x].contains("]")) {
                        msg.append(args[x], 0, args[x].length() - 1);
                        break;
                    }
                    else
                        msg.append(args[x]);
                    msg.append(" ");
                }

                publisher = (PublisherInterface) reg.lookup("Pub" + id);
                publisher.put(topic.toString(), msg.toString());
                break;
            case "subscribe":
                // subscribe id [topic]
                for (int x = 2; x < args.length; x++) {
                    index++;
                    if (args[x].contains("[")) {
                        if (args[x].contains("]")) {
                            topic.append(args[x], 1, args[x].length()-1);
                            break;
                        }
                        topic.append(args[x].substring(1));
                    }
                    else if (args[x].contains("]")) {
                        topic.append(args[x], 0, args[x].length() - 1);
                        break;
                    }
                    else
                        topic.append(args[x]);
                    topic.append(" ");
                }

                subscriber = (SubscriberInterface) reg.lookup("Sub" + id);
                subscriber.subscribe(topic.toString());
                break;
            case "unsubscribe":
                // unsubscribe id [topic]
                for (int x = 2; x < args.length; x++) {
                    index++;
                    if (args[x].contains("[")) {
                        if (args[x].contains("]")) {
                            topic.append(args[x], 1, args[x].length()-1);
                            break;
                        }
                        topic.append(args[x].substring(1));
                    }
                    else if (args[x].contains("]")) {
                        topic.append(args[x], 0, args[x].length() - 1);
                        break;
                    }
                    else
                        topic.append(args[x]);
                    topic.append(" ");
                }

                subscriber = (SubscriberInterface) reg.lookup("Sub" + id);
                subscriber.unsubscribe(topic.toString());
                break;
            case "get":
                // get id [topic]
                for (int x = 2; x < args.length; x++) {
                    if (args[x].contains("[")) {
                        if (args[x].contains("]")) {
                            topic.append(args[x], 1, args[x].length()-1);
                            break;
                        }
                        topic.append(args[x].substring(1));
                    }
                    else if (args[x].contains("]")) {
                        topic.append(args[x], 0, args[x].length() - 1);
                        break;
                    }
                    else
                        topic.append(args[x]);
                    topic.append(" ");
                }

                subscriber = (SubscriberInterface) reg.lookup("Sub" + id);
                subscriber.get(topic.toString());
                break;
            default:
                throw new IllegalArgumentException("Illegal argument" + args[1]);

        }
    }
}