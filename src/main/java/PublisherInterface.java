import java.rmi.Remote;
import java.rmi.RemoteException;

public interface PublisherInterface extends Remote {
    void put(String topic, String message) throws RemoteException;
}
