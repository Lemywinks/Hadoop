package hdfs;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.Naming;
import java.util.Properties;

public class HdfsServer {
	public static int hdfsPort;
	public static int hidoopPort;
	public static String daemonAdress;
	public static String nameNodeAdressPort;
	public static String pathToData;

	public static void loadConfig(String pathFile) {
		Properties prop = new Properties();
		try {
			// load a properties file
			prop.load(new FileInputStream(pathFile));
			// get the property value and print it out
			hdfsPort = Integer.parseInt(prop.getProperty("hdfs_port"));
			hidoopPort = Integer.parseInt(prop.getProperty("hidoop_port"));
			daemonAdress = prop.getProperty("daemonAdress");
			pathToData = prop.getProperty("pathData");
            nameNodeAdressPort = prop.getProperty("nameNodeAdressPort");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	public static void main(String[] args) {
	loadConfig(args[0]);

    // s'enregistrer aupr�s du NameNode
    DataNameNodeDaemon nnd;
    try {
        nnd = (DataNameNodeDaemon) Naming.lookup("//"+nameNodeAdressPort+"/DataNameNodeDaemon");
        System.out.println(pathToData);
        nnd.registerDaemon(new Node(daemonAdress, hidoopPort ,hdfsPort, pathToData));
    } catch (Exception e) {
        e.printStackTrace();
    }

    ServerSocket server = null;
	try {
        server = new ServerSocket(hdfsPort);// Serveur en �coute
        System.out.println("server en l'�coute sur le hdfsPort " + hdfsPort);
        Socket client;

        while(true) {
            client = server.accept();
            // chaque interaction avec client est trait� par une Thread InteractionClient
            InteractionClient ic = new InteractionClient(client, pathToData);
            new Thread(ic).start();
        }
    } catch (IOException e) {
		System.out.println("Error : connection failed");
		e.printStackTrace();
	}


	}
}
