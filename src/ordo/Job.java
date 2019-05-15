package ordo;

import java.io.File;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import application.MyMapReduce;
import config.Project;
import formats.Format;
import formats.Format.Type;
import formats.KVFormat;
import formats.LineFormat;
import hdfs.HdfsClient;
import hdfs.NameNodeDaemon;
import hdfs.Node;
import map.MapReduce;

public class Job implements JobInterfaceX, JobInterface {


	private int numberOfReduces;
	private int numberOfMaps;
	private Type inputFormat;
	private Type outputFormat;
	private String inputFname;
	private String outputFname;
	private SortComparator sortComparator;
	private static ArrayList<CallBack> lCallBacks;


	/* Création de la liste des CallBacks (On travaille sur 3 serveurs) */
	public Job() {
		this.numberOfMaps = 3;
		this.lCallBacks = new ArrayList<CallBack>();
	}


	@Override
	public void setNumberOfReduces(int tasks) {
		this.numberOfReduces = tasks;
	}

	@Override
	public void setNumberOfMaps(int tasks) {
		this.numberOfMaps = tasks;
	}

	@Override
	public void setInputFormat(Type ft) {
		this.inputFormat = ft;
	}

	@Override
	public void setOutputFormat(Type ft) {
		this.outputFormat = ft;
	}

	@Override
	public void setInputFname(String fname) {
		this.inputFname = fname;
	}

	@Override
	public void setOutputFname(String fname) {
		this.outputFname = fname;
	}

	@Override
	public void setSortComparator(SortComparator sc) {
		this.sortComparator = sc;
	}

	@Override
	public int getNumberOfReduces() {
		return this.numberOfReduces;
	}

	@Override
	public int getNumberOfMaps() {
		return this.numberOfMaps;
	}

	@Override
	public Type getInputFormat() {
		return this.inputFormat;
	}

	@Override
	public Type getOutputFormat() {
		return this.outputFormat;
	}

	@Override
	public String getInputFname() {
		return this.inputFname;
	}

	@Override
	public String getOutputFname() {
		return this.outputFname;
	}

	@Override
	public SortComparator getSortComparator() {
		return this.sortComparator;
	}


	@Override
	public void startJob(MapReduce mr) {

		/* Création du fichier résultat dans la machine locale */
		String nameRes = this.getInputFname() + "-res";
		File fileRes = new File(nameRes);
		try {
			if (fileRes.createNewFile()) {
				System.out.println("Fichier Résultat crée avec succés");
			}
			else {
					System.out.println("Echec lors de la création du fichier Résultat");
			}
		} catch(Exception e) {
			e.printStackTrace();
		}

		Hashtable<Integer, String> fragments = new Hashtable<Integer, String>();
		Node[] nodes;

		try {
			String URL_NameNode = "//localhost:" + 4141 + "/NameNodeDaemon";
			System.out.println(URL_NameNode);
			NameNodeDaemon daemon_NameNode = (NameNodeDaemon) Naming.lookup(URL_NameNode);
			nodes = daemon_NameNode.getDaemons();
			int i = 1;
			for (Node node : nodes) {
				fragments.put(i, node.getPathData());
				i = i+1;
			}
			System.out.println(fragments);
		} catch (MalformedURLException | RemoteException | NotBoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}



		/* Faire la liaison avec les machines distantes (On considère qu'on a 3 serveurs) */
		int numServeur = 1;

		/* Création de l'exécuteur des Threads */
		ExecutorService executeur = Executors.newFixedThreadPool(this.numberOfMaps);
		try {
		for (String nomServeur : Project.lServeurs.keySet()) {
			System.out.println(fragments.get(numServeur));

			/* Création du nameReader et nameWriter */
			String nameReader = fragments.get(numServeur);
			String nameWriter = nameReader + "-mapped";

			/* Création du Reader et Writer */
			Format Reader = new LineFormat(nameReader);
			Format Writer = new KVFormat(nameWriter);

			/* Lier le serveur distant nomServeur */
			String URL = "//" + InetAddress.getLocalHost().getHostName()+":" + Project.lServeurs.get(nomServeur) + "/" + nomServeur;
			System.out.println(URL);
			Daemon daemon = (Daemon) Naming.lookup(URL);

			/* Création du CallBack */
			CallBack cb = new CallBackImpl();
			this.lCallBacks.add(cb);

			/* Exécuter le Thread */
			executeur.execute(new ExecThread(daemon,Reader,Writer,cb) {
				@Override
				public void run () {
					/* Exécution du map sur le daemon */
					try {
						daemon.runMap(mr,Reader,Writer,cb);
					} catch (RemoteException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			});
			numServeur++;

		}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}

		/* Attendre la fin de l'exécution du Map sur tous les Thread */
		for (int i = 0; i < 2; i++) {
			try {
				this.lCallBacks.get(i).getCall().acquire();	// Vérifier que le CallBack est terminé
				System.out.println("L'exécution du Map sur le " + i+1 + "ème Thread a teminé");
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Map est bien exécutée sur tous les serveurs");
		/* On exécute alors les reduce */

		/* Exécution de HdfsRead sur le fichier Résultat */

		HdfsClient.HdfsRead(nameRes, "/home/melalaou/nosave/test_hidoop/test_job/filesample.txt" + "-mapped");
		System.out.println("Les résultats d'exécution de Map ont été fusionnées");

		Format ReaderRes = new KVFormat("/home/melalaou/nosave/test_hidoop/test_job/filesample.txt"+ "-mapped");
		Format WriterRes = new KVFormat(nameRes);

		mr.reduce(ReaderRes, WriterRes);
		WriterRes.close();

		System.out.println("Exécution du Reduce est terminée. Le fichier " + nameRes + " a été crée");

	}
	public static void main(String[] args) {
		Job job = new Job();
		job.setInputFormat(Format.Type.LINE);
		//job.setInputFname("/home/melalaou/workspace_jee/hidoop/tro");
		job.startJob(new MyMapReduce());
	}
}


