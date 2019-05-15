/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.rmi.Naming;
import java.util.Properties;

import application.MyMapReduce;
import config.Project;
import formats.Format;
import ordo.Job;
import ordo.JobInterface;

public class HdfsClient {
    private final static String config_path= "/home/melalaou/workspace_jee/hidoopv3/src/config/localhost.properties";
	private static int portNameNode;
	private static String IPNameNode;
	private static String pathRead;

    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }

    private static Node[] getDaemons() {
		try {
            NameNodeDaemon nnd = (NameNodeDaemon) Naming.lookup("//" + IPNameNode + ":" + portNameNode + "/NameNodeDaemon");
            return nnd.getDaemons();
        } catch (Exception e) {
        	System.out.println("erreur r�cup�ration de la liste des noeuds");
        }
		return null;
	}

    public static void HdfsDelete(String hdfsFname) {
    	loadConfig(config_path);
		try {
            NameNodeDaemon nnd = (NameNodeDaemon) Naming.lookup("//" + IPNameNode + ":" + portNameNode + "/NameNodeDaemon");
            Fichier f = nnd.locateFile(hdfsFname);
            int n= f.getNumberChunk();
            for(int i = 0; i<n ; i++){
            	try {
            		Chunk chu = f.getListChunks().get(i);
            		String commande = Commande.CMD_DELETE+" "+hdfsFname.length()+" "+hdfsFname;
            		Socket sServer = new Socket(chu.getNodeId().getNodeIP(),chu.getNodeId().getHdfsPort());
            		OutputStream osS = sServer.getOutputStream();
            		ObjectOutputStream oOsS = new ObjectOutputStream(osS);
            		oOsS.writeObject(commande);

            		oOsS.close();
            		sServer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		nnd.supFichier(hdfsFname);
	}
        } catch (Exception e) {
        System.out.println("erreur dans la r�cup�ration du fichier");
        }
    }

    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor ) {
    	loadConfig(config_path);
    	File f = new File(localFSSourceFname);
    	Node[] listeNodes = getDaemons();
    	int n = listeNodes.length;
    	for(int i = 0; i<n; i++){
    		try {
    			Node nd = listeNodes[i];
        		String commande = Commande.CMD_READ+" "+localFSSourceFname;
        		Socket sServer = new Socket(nd.getNodeIP(),nd.getHdfsPort());
        		OutputStream osS = sServer.getOutputStream();
        		ObjectOutputStream oOsS = new ObjectOutputStream(osS);
        		oOsS.writeObject(commande);

        		int nbligne = getNbLines(localFSSourceFname);
				emmisionf(oOsS,localFSSourceFname,nbligne);
        		//CreateJob(Commande.CMD_WRITE,localFSSourceFname);

			} catch (IOException e) {
				e.printStackTrace();
			}
        }
	}



	private static void emmisionf(ObjectOutputStream oos, String fName, int nbligne) {

		String ligne;
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(Project.PATHF+fName)));

			while(( ligne = br.readLine()) != null){
					oos.writeObject(ligne);
			}
			oos.writeObject(null);
			if( br!=null)
				br.close();
			System.out.println("Envoi termin� au server");
		}catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static int getNbLines(String localFSSourceFname) {
		int nbligne = 0;
    	String ligne;
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(Project.PATHF+localFSSourceFname)));

			while(( ligne = br.readLine()) != null){
				nbligne++;
			}
				br.close();
		}catch (IOException e) {

			e.printStackTrace();
		}
		return nbligne;
	}

	public static void loadConfig(String pathFile) {
		Properties prop = new Properties();

		try {
			// load a properties file
			prop.load( new FileInputStream(pathFile));
			// get the property value and print it out
			portNameNode = 4141;
            IPNameNode = prop.getProperty("adresseNameNode");
            pathRead = prop.getProperty("pathRead");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	public static void HdfsRead(String hdfsFname, String localFSDestFname) {

		loadConfig(config_path);
		try {
            NameNodeDaemon nnd = (NameNodeDaemon) Naming.lookup("//" + IPNameNode + ":" + portNameNode + "/NameNodeDaemon");
            Fichier f = nnd.locateFile(hdfsFname);
        	FileWriter fw = new FileWriter("/home/melalaou/nosave/test_hidoop/test_job/filesample.txt-res");
			BufferedWriter bw = new BufferedWriter(fw);
            int n = nnd.getDaemons().length;
            for(int i = 0; i<n; i++){
        		try {
            		Node nd = nnd.getDaemons()[i];
            		String commande = Commande.CMD_READ+" "+hdfsFname.length()+" "+hdfsFname;
            		Socket sServer = new Socket(nd.getNodeIP(),nd.getHdfsPort());
            		OutputStream osS = sServer.getOutputStream();
            		InputStream isS = sServer.getInputStream();
            		ObjectOutputStream oOsS = new ObjectOutputStream(osS);
            		ObjectInputStream oIsS = new ObjectInputStream(isS);
            		oOsS.writeObject(commande);
            		String line = (String) oIsS.readObject();
            		bw.write(line);
            		oOsS.close();
            		oIsS.close();
            		sServer.close();

            		CreateJob(Commande.CMD_READ,hdfsFname);

    			} catch (IOException e) {
    				e.printStackTrace();
    			}
            }
		}
        catch (Exception e) {
        System.out.println("erreur dans la r�cup�ration du fichier");

        }
     }



    private static void CreateJob(Commande cmd,String fName){

    	JobInterface j;
    	switch(cmd){
    	case CMD_WRITE:
        	j = new Job();
    		break;
    	case CMD_READ:
        	j = new Job();
    		break;
		default:
			j = new Job();
    	}
        j.setInputFormat(Format.Type.LINE);
        j.setInputFname(fName);
		j.startJob(new MyMapReduce());
    }

    public static void main(String[] args) {
        // java HdfsClient <read|write> <line|kv> <file>

        try {
            if (args.length<2) {usage(); return;}

            switch (args[0]) {
              case "read": HdfsRead(args[2],args[2]); break;
              case "delete": HdfsDelete(args[1]); break;
              case "write":
                Format.Type fmt;
                if (args.length<3) {usage(); return;}
                if (args[1].equals("line")) fmt = Format.Type.LINE;
                else if(args[1].equals("kv")) fmt = Format.Type.KV;
                else {usage(); return;}
                HdfsWrite(fmt,args[2],1);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
