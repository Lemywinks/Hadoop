package hdfs;

import java.io.Serializable;

public class Chunk implements Serializable{
 
	/**
	 * 
	 */
	private static final long serialVersionUID = 3292944377908200948L;
	protected Node node;
	protected String chunkLocalname; //"fname"
	protected long taille;

	public Chunk(Node node , long taille,String chunkLocalname) {
	super();
	this.node = node;
	this.taille = taille;
	this.chunkLocalname = chunkLocalname;
	}



	public Node getNodeId() {
		return node;
	}


	public String getChunkLocalname() {
		return chunkLocalname;
	}

	public long getTaille() {
		return taille;
	}

	public void setChunkLocalname(String chunkLocalname) {
		this.chunkLocalname = chunkLocalname;
	}


}