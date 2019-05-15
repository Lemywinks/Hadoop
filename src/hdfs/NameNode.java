package hdfs;

import java.io.FileInputStream;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class NameNode extends UnicastRemoteObject implements NameNodeDaemon,DataNameNodeDaemon {
	private static final long serialVersionUID = 1L;
	public static int portNameNode;
	public static String pathMetaData;
	public Map<String,Node> dataNodes = new HashMap<>();
	public static Map<String,Fichier> mapFichier = new HashMap<String,Fichier>();


	protected NameNode() throws RemoteException {
		super();
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args)  {
		if (args.length != 1) {
			return;
		}
		loadConfig(args[0]);

        try {
            int rmiPort = portNameNode;
            LocateRegistry.createRegistry(rmiPort);
            NameNode nameNodeInstance = new NameNode();
            Naming.rebind("//localhost:" + rmiPort + "/NameNodeDaemon", nameNodeInstance);
            Naming.rebind("//localhost:" + rmiPort + "/DataNameNodeDaemon", nameNodeInstance);
            System.out.println("NameNode RMI en écoute sur le hdfsPort " + rmiPort);
        } catch (Exception e) {
		    // TODO throw error
            e.printStackTrace();
        }
}
	private String NodeToKey(Node node) {
		// TODO Auto-generated method stub
		return node.getNodeIP() + ":" + node.getHdfsPort();
	}
	public static void loadConfig(String pathFile) {
		Properties prop = new Properties();

		try {
			// load a properties file
			prop.load( new FileInputStream(pathFile));
			// get the property value and print it out
			portNameNode=Integer.parseInt(prop.getProperty("portNameNode"));
			pathMetaData=prop.getProperty("pathMetaData");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	@Override
	public Node[] getDaemons() throws RemoteException {
		System.out.println("Requete de la liste des d�mons. (nombre de d�mon "+ dataNodes.size() +")");
        return dataNodes.values().toArray(new Node[dataNodes.size()]);
	}
	@Override
	public void registerDaemon(Node newDaemon) throws RemoteException {
		 System.out.println("Enregistrement d'un nouveau démon " + newDaemon.getNodeIP() + ":" + newDaemon.getHidoopPort()+"|" + newDaemon.getHdfsPort());
		dataNodes.put(NodeToKey(newDaemon),newDaemon);

	}

	@Override
	public Fichier locateFile(String hdfsFname) throws RemoteException{
		// TODO Auto-generated method stub
		return mapFichier.get(hdfsFname) ;
	}

	@Override
	public void supFichier(String fname) throws RemoteException{
		// TODO Auto-generated method stub
		mapFichier.remove(fname);
	}
}


