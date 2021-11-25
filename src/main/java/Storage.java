import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
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
}
