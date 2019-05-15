package hdfs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import formats.Format;

public class Fichier implements Serializable {
	/**
	 * class File contient les donn�es du fichier
	 *
	 */
	protected String fName;
	protected long taille;
	protected List<Chunk> listChunks;
	protected Format.Type format;

    public Fichier(String fname, long taille, formats.Format.Type format) {

    	this.fName = fname;
        this.taille = taille;
        this.format = format;
        this.listChunks = new ArrayList<Chunk>();

    }

    public void addChunk(Chunk ch) {

        this.listChunks.add(ch);

    }

    @Override
    public String toString() {
        return "File [fName=" + fName + ", taille=" + taille + ", format=" + format + ", nbChunks=" + listChunks.size()
                + "]";
    }

	public String getfName() {
		return fName;
	}

	public long getTaille() {
		return taille;
	}

	public List<Chunk> getListChunks() {
		return listChunks;
	}

	public Format.Type getFormat() {
		return format;
	}

	public void setfName(String fName) {
		this.fName = fName;
	}

	public void setTaille(long taille) {
		this.taille = taille;
	}

    public int getNumberChunk() {
        return listChunks.size();
    }

	public void setListChunks(List<Chunk> listechunks) {
		// TODO Auto-generated method stub
		this.listChunks = listechunks;
	}


}
