package hdfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DataNameNodeDaemon extends Remote {

	void registerDaemon(Node nodeId) throws RemoteException;

}
