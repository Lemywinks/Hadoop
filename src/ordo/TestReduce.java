package ordo;

import java.util.ArrayList;

import application.SecondarySortApp;
import formats.Format;
import formats.Format.Type;
import formats.KVFormat;
import map.MapReduce;

public class TestReduce implements JobInterfaceX, JobInterface {


	private int numberOfReduces;
	private int numberOfMaps;
	private Type inputFormat;
	private Type outputFormat;
	private String inputFname;
	private String outputFname;
	private SortComparator sortComparator;
	private static ArrayList<CallBack> lCallBacks;


	/* Création de la liste des CallBacks (On travaille sur 3 serveurs) */
	public TestReduce() {
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


		String nameReader = this.inputFname;

		String nameWriter = this.inputFname + "-res" ;

		/* On exécute alors les reduce */


		Format ReaderRes = new KVFormat(nameReader);
		Format WriterRes = new KVFormat(nameWriter);

		mr.reduce(ReaderRes, WriterRes);
		WriterRes.close();

		System.out.println("Exécution du Reduce est terminée. Le fichier " + nameWriter + " a été crée");

	}

		public static void main(String[] args) {
			TestReduce job = new TestReduce();
			job.setInputFormat(Format.Type.KV);
			job.setInputFname("/home/melalaou/nosave/test_hidoop/test_secondarysort/filesample_frag1_LINE.txt-mapped");
			job.startJob(new SecondarySortApp());
		}




}


