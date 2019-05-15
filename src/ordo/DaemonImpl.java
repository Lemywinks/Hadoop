package ordo;

import java.net.InetAddress;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import formats.Format;
import map.Mapper;

public class DaemonImpl extends UnicastRemoteObject implements Daemon {


	public DaemonImpl() throws RemoteException {
	}


	@Override
	public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {

		/* Executer le map sur le reader et le writer */
		m.map(reader, writer);
		System.out.println("Execution de map terminée avec succés");

		/* Fermer le writer */
		writer.close();

		/* Envoyer le CallBack */
		cb.onFinished();
		System.out.println("CallBack envoyé avec succes");
	}


	public static void main(String args[]) {

		/* Numero du port et l'adresse de la machine */
		int port;
		String URL;

		/* Récupérer le port */
		try { 		// La classe Integer peut soulever une exception
			Integer I = new Integer(args[0]);		// Stocker l'argumer sur un objet Integer
			port = I.intValue();
		} catch (Exception exception) {
			System.out.println("Mauvaise utilisation de Integer");
			return;
		}



		try {

			/* Creation du registry */
			Registry registry = LocateRegistry.createRegistry(port);
			System.out.println("Registry crée");

			/* Calculer l'URL de la machine */
			URL = "//" + InetAddress.getLocalHost().getHostName()+":" + port + "/" + args[1];
			System.out.println(URL + "calculé");
			Naming.rebind(URL, new DaemonImpl());
		}
		catch (Exception e) {
			e.printStackTrace();
		}

	}

}
