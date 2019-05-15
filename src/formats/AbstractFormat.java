package formats;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public abstract class AbstractFormat implements Format {

	protected long index;
	private String filename;
	private File file;
	private FileReader fReader;
	protected boolean openReader;
	protected BufferedReader bReader;
	private FileWriter fWriter;
	protected BufferedWriter bWriter;
	protected boolean openWriter;
	private String type;

	public AbstractFormat(String name) {
		this.filename = name;
		this.file = new File(name);
		this.index = 0;
	}

	@Override
	public void open(OpenMode mode) {
			if (mode == Format.OpenMode.R) {
				try {
					this.fReader = new FileReader(this.file);
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
				this.bReader = new BufferedReader(fReader);
				this.openReader = true;		// Le fichier est ouvert
			}
			if (mode == Format.OpenMode.W) {
				try {
					this.fWriter = new FileWriter(this.file);
				} catch (IOException e) {
					e.printStackTrace();
				}
				this.bWriter = new BufferedWriter(this.fWriter);
				this.openWriter = true;		// Le fichier est ouvert
			}
	}

	@Override
	public void close() {
		if (this.openReader) {
			this.openReader = false;		// Le fichier est fermé
			try {
				this.bReader.close();
				this.fReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (this.openWriter) {
			this.openWriter = false;		// Le fichier est fermé
			try {
				this.bWriter.close();
				this.fWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
		this.index = 0;
	}



	@Override
	public long getIndex() {
		return this.index;
	}

	@Override
	public String getFname() {
		return this.filename;
	}

	@Override
	public void setFname(String fname) {
		this.filename = fname;

	}

	public String getType() {
		return this.type;
	}

	public void setType(String type) {
		this.type = type;
	}

}
