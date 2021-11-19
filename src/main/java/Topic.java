import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

public class Topic {
    private String name;
    private int messageId;
    private ArrayList<Integer> subscribers;
    private ConcurrentHashMap<Integer, String> messages; // <messageId, message>
    private ConcurrentHashMap<Integer, Integer> subsLastMessage; // <subId, messageId>
    private ArrayList<Integer> messagesIds;
    private ArrayList<Integer> subsLastMessageIds;

    public Topic(String name) {
        this.name = name;
        this.messageId = 0;
        this.subscribers = new ArrayList<>();
        this.messages = new ConcurrentHashMap<>();
        this.subsLastMessage = new ConcurrentHashMap<>();
        this.messagesIds = new ArrayList<>();
        this.subsLastMessageIds = new ArrayList<>();
    }

    public void addSubscriber(int id) {
        this.subscribers.add(id);
        this.subsLastMessage.put(id, this.messageId);
        this.subsLastMessageIds.add(this.messageId);
    }

    public void removeSubscriber(int id) {
        if(!subscribers.contains(id)) {
            System.out.println("Subscriber " + id + " already unsubscribed topic " + this.name);
            return;
        }
        this.subscribers.remove(Integer.valueOf(id));
        int messageIdSub = this.subsLastMessage.get(id);
        this.subsLastMessage.remove(Integer.valueOf(id));
        this.subsLastMessageIds.remove(messageIdSub);
    }

    public void addMessage(String message) {
        this.messages.put(this.messageId, message);
        this.messagesIds.add(this.messageId);
        this.messageId++;
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

    public String get_message(int subId) {
        int lastSendMessageId = this.subsLastMessage.get(subId);

        if(lastSendMessageId >= Collections.max(this.messagesIds)) {
            return "Null";
        }

        this.subsLastMessage.replace(subId, lastSendMessageId + 1);
        String message = this.messages.get(Integer.valueOf(lastSendMessageId + 1));
        this.subsLastMessageIds.remove(Integer.valueOf(lastSendMessageId));
        this.subsLastMessageIds.add(lastSendMessageId + 1);

        this.removeMessage();

        return message;
    }
}
