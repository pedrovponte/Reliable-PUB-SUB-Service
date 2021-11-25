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
        int id = 0;
        try {
            id = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("Invalid ID. Try again.");
            return;
        }

        PublisherInterface publisher;
        SubscriberInterface subscriber;
        int index = 2;
        StringBuilder topic = new StringBuilder();
        StringBuilder msg = new StringBuilder();
        int rightBracketCnt = 0, leftBracketCnt = 0;

        switch (args[0].toLowerCase()) {
            case "put":
                // put id [topic] [message]

                for (String arg: args) {
                    if (arg.contains("[") || arg.contains("]")) {
                        for (int x = 0; x < arg.length(); x++) {
                            char c = arg.charAt(x);
                            if (String.valueOf(c).equals("["))
                                leftBracketCnt++;
                            else if (String.valueOf(c).equals("]"))
                                rightBracketCnt++;
                        }
                    }
                }

                if (args.length < 4 || rightBracketCnt != 2 || leftBracketCnt != 2) {
                    System.out.println("PUT USAGE: put <id> [<topic>] [<message>]");
                    return;
                }

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

                try {
                    publisher = (PublisherInterface) reg.lookup("Pub" + id);
                    publisher.put(topic.toString(), msg.toString());
                }
                catch (RemoteException e) {
                    System.out.println("There is no publisher with that ID. Try again.");
                }
                break;
            case "subscribe":
                // subscribe id [topic]
                for (String arg: args) {
                    if (arg.contains("[") || arg.contains("]")) {
                        for (int x = 0; x < arg.length(); x++) {
                            char c = arg.charAt(x);
                            if (String.valueOf(c).equals("["))
                                leftBracketCnt++;
                            else if (String.valueOf(c).equals("]"))
                                rightBracketCnt++;
                        }
                    }
                }

                if (args.length < 3 || rightBracketCnt != 1 || leftBracketCnt != 1) {
                    System.out.println("SUBSCRIBE USAGE: subscribe <id> [<topic>]");
                    return;
                }

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

                try {
                    subscriber = (SubscriberInterface) reg.lookup("Sub" + id);
                    subscriber.get(topic.toString());
                }
                catch (RemoteException e) {
                    System.out.println("There is no subscriber with that ID. Try again.");
                }
                break;
            case "unsubscribe":
                // unsubscribe id [topic]
                for (String arg: args) {
                    if (arg.contains("[") || arg.contains("]")) {
                        for (int x = 0; x < arg.length(); x++) {
                            char c = arg.charAt(x);
                            if (String.valueOf(c).equals("["))
                                leftBracketCnt++;
                            else if (String.valueOf(c).equals("]"))
                                rightBracketCnt++;
                        }
                    }
                }

                if (args.length < 3 || rightBracketCnt != 1 || leftBracketCnt != 1) {
                    System.out.println("UNSUBSCRIBE USAGE: unsubscribe <id> [<topic>]");
                    return;
                }

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

                try {
                    subscriber = (SubscriberInterface) reg.lookup("Sub" + id);
                    subscriber.unsubscribe(topic.toString());
                }
                catch (RemoteException e) {
                    System.out.println("There is no subscriber with that ID. Try again.");
                }
                break;
            case "get":
                // get id [topic]
                for (String arg: args) {
                    if (arg.contains("[") || arg.contains("]")) {
                        for (int x = 0; x < arg.length(); x++) {
                            char c = arg.charAt(x);
                            if (String.valueOf(c).equals("["))
                                leftBracketCnt++;
                            else if (String.valueOf(c).equals("]"))
                                rightBracketCnt++;
                        }
                    }
                }

                if (args.length < 3 || rightBracketCnt != 2 || leftBracketCnt != 2) {
                    System.out.println("GET USAGE: get <id> [<topic>]");
                    return;
                }

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

                try {
                    subscriber = (SubscriberInterface) reg.lookup("Sub" + id);
                    subscriber.get(topic.toString());
                }
                catch (RemoteException e) {
                    System.out.println("There is no subscriber with that ID. Try again.");
                }
                break;
            default:
                throw new IllegalArgumentException("Illegal argument" + args[1]);

        }
    }
}