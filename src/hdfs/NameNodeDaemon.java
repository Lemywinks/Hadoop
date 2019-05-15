package hdfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface NameNodeDaemon extends Remote {

	Node[] getDaemons() throws RemoteException;

	Fichier locateFile(String hdfsFname) throws RemoteException;

	void supFichier(String hdfsFname) throws RemoteException;

}
