import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

public class Topic implements Serializable {
    private String name;
    private int messageId;
    private ArrayList<Integer> subscribers;
    private ConcurrentHashMap<Integer, String> messages; // <messageId, message>
    private ConcurrentHashMap<Integer, Integer> subsLastMessage; // <subId, messageId>
    private ArrayList<Integer> messagesIds;
    private ArrayList<Integer> subsLastMessageIds;

    public Topic(String name) {
        this.name = name;
        this.messageId = -1;
        this.subscribers = new ArrayList<>();
        this.messages = new ConcurrentHashMap<>();
        this.subsLastMessage = new ConcurrentHashMap<>();
        this.messagesIds = new ArrayList<>();
        this.subsLastMessageIds = new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public ArrayList<Integer> getSubscribers() {
        return subscribers;
    }

    public ConcurrentHashMap<Integer, String> getMessages() {
        return messages;
    }

    public boolean hasSubscriber(int subID) {
        return this.subscribers.contains(subID);
    }

    public void addSubscriber(int id) {
        this.subscribers.add(id);
        this.subsLastMessage.put(id, this.messageId);
        this.subsLastMessageIds.add(this.messageId);
        /*System.out.println("Subscribers: " + subscribers);
        System.out.println("Subs Last Message: " + this.subsLastMessage);
        System.out.println("Subs Last Message Ids: " + this.subsLastMessageIds);*/
    }

    public void removeSubscriber(int id) {
        if(!subscribers.contains(id)) {
            System.out.println("Subscriber " + id + " already unsubscribed topic " + this.name);
            return;
        }
        this.subscribers.remove(Integer.valueOf(id));
        int messageIdSub = this.subsLastMessage.get(id);
        this.subsLastMessage.remove(id);
        this.subsLastMessageIds.remove(Integer.valueOf(messageIdSub));

        /*System.out.println("Subscribers: " + subscribers);
        System.out.println("Subs Last Message: " + this.subsLastMessage);
        System.out.println("Subs Last Message Ids: " + this.subsLastMessageIds);*/
    }

    public void addMessage(String message) {
        this.messageId++;
        this.messages.put(this.messageId, message);
        this.messagesIds.add(this.messageId);
    }

    public int removeMessage() {
        int oldestMessageId = Collections.min(this.messagesIds);
        int oldestSubsMessageId = Collections.min(this.subsLastMessageIds);

        if(oldestMessageId < oldestSubsMessageId) {
            this.messages.remove(oldestMessageId);
            this.messagesIds.remove(Integer.valueOf(oldestMessageId));
            return removeMessage();
        }

        return 0;
    }

    public boolean checkNext(int subId) {
        int lastSendMessageId = this.subsLastMessage.get(subId);

        if(lastSendMessageId >= Collections.max(this.messagesIds)) {
            return false;
        }
        return true;
    }

    public String getMessage(int subId) {
        int lastSendMessageId = this.subsLastMessage.get(subId);

//        if(lastSendMessageId >= Collections.max(this.messagesIds)) {
//            return null;
//        }

        this.subsLastMessage.replace(subId, lastSendMessageId + 1);
        String message = this.messages.get(lastSendMessageId + 1);
        this.subsLastMessageIds.remove(Integer.valueOf(lastSendMessageId));
        this.subsLastMessageIds.add(lastSendMessageId + 1);

        /*System.out.println("Subscribers: " + subscribers);
        System.out.println("Subs Last Message: " + this.subsLastMessage);
        System.out.println("Subs Last Message Ids: " + this.subsLastMessageIds);*/

        this.removeMessage();

        return message;
    }
}
