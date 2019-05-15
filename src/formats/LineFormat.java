package formats;

import java.io.IOException;

public class LineFormat extends AbstractFormat {

	public LineFormat(String name) {
		super(name);
	}

	@Override
	public KV read() {
		KV record = new KV();

		if (!this.openReader) {
			this.open(OpenMode.R);
		}
		try {
			String ligne = this.bReader.readLine();
			if (ligne != null) {
				record = new KV(Integer.toString((int)this.index),ligne);
				this.index++;
			} else {
				record = null;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return record;
	}

	@Override
	public void write(KV record) {
		if (!this.openWriter) {
			this.open(OpenMode.W);
		}
		try {
			String nligne = record.v;
			bWriter.write(nligne +"\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.index++;
	}

	@Override
	public String getType() {
		return "Line";
	}

}
