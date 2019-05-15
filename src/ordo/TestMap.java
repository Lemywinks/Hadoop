package ordo;

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

import application.SecondarySortApp;
import config.Project;
import formats.Format;
import formats.Format.Type;
import formats.KVFormat;
import formats.LineFormat;
import map.MapReduce;

public class TestMap implements JobInterfaceX, JobInterface {


	private int numberOfReduces;
	private int numberOfMaps;
	private Type inputFormat;
	private Type outputFormat;
	private String inputFname;
	private String outputFname;
	private SortComparator sortComparator;
	private static ArrayList<CallBack> lCallBacks;


	/* Création de la liste des CallBacks (On travaille sur 3 serveurs) */
	public TestMap() {
		this.numberOfMaps = 2;
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



		/* Faire la liaison avec les machines distantes (On considère qu'on a 3 serveurs) */
		int numServeur = 1;

		/* Création de l'exécuteur des Threads */
		ExecutorService executeur = Executors.newFixedThreadPool(this.numberOfMaps);
		try {

			String f1 = "/home/melalaou/nosave/test_hidoop/test_secondarysort/filesample_frag1_LINE.txt";
			String f2 = "/home/melalaou/nosave/test_hidoop/test_secondarysort/filesample_frag2_LINE.txt";

			Hashtable<Integer, String> fragments = new Hashtable<Integer, String>();
			fragments.put(1, f1);
			fragments.put(2, f2);


		for (String nomServeur : Project.lServeurs.keySet()) {


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
		for (int i = 0; i < this.numberOfMaps; i++) {
			try {
				this.lCallBacks.get(i).getCall().acquire();	// Vérifier que le CallBack est terminé
				System.out.println("L'exécution du Map sur le Thread " + (i+1) + " a teminé");
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Map est bien exécutée sur tous les serveurs");

		/* On exécute alors les reduce */

		/* Exécution de HdfsRead sur le fichier Résultat */
		//HdfsClient.HdfsRead(nameRes, "filesample.txt");
		//System.out.println("Les résultats d'exécution de Map ont été fusionnées");

		//Format ReaderRes = new KVFormat("filesample.txt");
		//Format WriterRes = new KVFormat(nameRes);

		//mr.reduce(ReaderRes, WriterRes);
		//WriterRes.close();

		//System.out.println("Exécution du Reduce est terminée. Le fichier " + nameRes + " a été crée");

	}

		public static void main(String[] args) {
			TestMap job = new TestMap();
			job.setInputFormat(Format.Type.LINE);
			job.setInputFname("~/fichier");
	        long t1 = System.currentTimeMillis();
			job.startJob(new SecondarySortApp());
			long t2 = System.currentTimeMillis();
	        System.out.println("time in ms =" + (t2-t1));
	        System.exit(0);
		}




}


