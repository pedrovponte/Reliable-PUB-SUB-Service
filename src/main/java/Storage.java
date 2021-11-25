import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Storage implements Serializable {
    private ConcurrentHashMap<String, Topic> topics; // key -> topicName; value -> Topic

    public Storage() {
        this.topics = new ConcurrentHashMap<>();
    }

    public java.util.List<String> getTopicNames() {
        java.util.List<String> topicNames = new ArrayList<>();

        for (String t: topics.keySet())
            topicNames.add(topics.get(t).getName());

        return topicNames;
    }

    public ConcurrentHashMap<String, Topic> getTopics() {
        return topics;
    }

    public List<String> getTopicsSubscribedBySubscriber(int subID) {
        java.util.List<String> topics = new ArrayList<>();

        for (String t: this.topics.keySet()) {
            for (int id: this.topics.get(t).getSubscribers()) {
                if (id == subID)
                    topics.add(this.topics.get(t).getName());
            }
        }

        return topics;
    }
}
