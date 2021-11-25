import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class StorageSub implements Serializable {
    private ConcurrentHashMap<String, String> topicsSubscribed; // <topic, time last get>

    public StorageSub() {
        this.topicsSubscribed = new ConcurrentHashMap<>();
    }

    public ConcurrentHashMap<String, String> getTopicsSubscribed() {
        return this.topicsSubscribed;
    }

    public ArrayList<String> getTopics() {

        ArrayList<String> topics = new ArrayList<>(this.topicsSubscribed.keySet());

        return topics;
    }

    public String getTimeTopic(String topic) {
        return this.topicsSubscribed.get(topic);
    }

    public void replaceTimeTopic(String topic, String time) {
        this.topicsSubscribed.replace(topic, time);
    }

    public void addTopic(String topic, String time) {
        this.topicsSubscribed.put(topic, time);
    }

    public void removeTopic(String topic) {
        this.topicsSubscribed.remove(topic);
    }
}
