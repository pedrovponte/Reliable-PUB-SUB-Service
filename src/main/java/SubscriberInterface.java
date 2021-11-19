import java.rmi.Remote;
import java.rmi.RemoteException;

public interface SubscriberInterface extends Remote {
    void subscribe(String topic) throws RemoteException;
}
