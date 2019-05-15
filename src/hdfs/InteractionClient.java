package hdfs;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.rmi.Naming;

import formats.KV;
import formats.LineFormat;
import formats.Format;
import formats.Format.OpenMode;


public class InteractionClient implements Runnable{
	private Socket socket;
	private ObjectOutputStream out;
	private ObjectInputStream in;
	private String pathToData;


	public InteractionClient(Socket s, String pathToData) {
		this.pathToData = pathToData;
		this.socket = s;
		try {
			this.out = new ObjectOutputStream(socket.getOutputStream());
			this.in = new ObjectInputStream(socket.getInputStream());

		} catch (IOException e) {
			System.out.println("Error in creation of ObjectInputStream or ObjectOuputStream ");
			e.printStackTrace();
		}
	}
	@Override
	public void run() {
		try {
		// TODO Auto-generated method stub	
		ObjectInputStream ois = new ObjectInputStream (socket.getInputStream());
		ObjectOutputStream oos = new ObjectOutputStream (socket.getOutputStream());
		
		// Reception d'une commande type Commande + taille du nom du fichier + nom du fichier
		String message = (String) ois.readObject();
		String cmd = message.split(" ")[0];
		Commande com = Commande.valueOf(cmd);
		switch (com) {
			case CMD_WRITE :
				traiterWrite(message.split(" ")[2], ois,oos);
				break;
			case CMD_READ:
				traiterRead(message.split(" ")[2], oos);
				break;
			case CMD_DELETE:
				traiterDelete(message.split(" ")[2]);
				break;
			}
		}catch (Exception e) {
			System.out.println("e");
		}
		

	}
		
		public static void traiterWrite(String nomFichier,ObjectInputStream ois,ObjectOutputStream oos) {
			Format formatFrag = new LineFormat(nomFichier);
			formatFrag.setFname("frag_"+nomFichier);
			formatFrag.open(OpenMode.W);
			
			Object obj;
			int nb=0;
			try{
				while(( obj = ois.readObject()) != null ){
					String ligne = (String) obj;
					KV kv = new KV(String.valueOf(nb),ligne);
					nb++;
					formatFrag.write(kv);
				}
			}catch(EOFException e){
				//fini
			} catch (ClassNotFoundException|IOException e) {
				e.printStackTrace();
			} 

			
			formatFrag.close();

		}
		
		
		public static void traiterRead(String nomFichier, ObjectOutputStream oos) {
			
			try {
				oos.writeObject(nomFichier);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		}

		public static void traiterDelete (String nomFichier) {
			File frag = new File(nomFichier);
			boolean df = frag.delete();


		}
}


