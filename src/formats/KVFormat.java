package formats;

import java.io.IOException;

public class KVFormat extends AbstractFormat {

	public KVFormat(String name) {
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
				String [] lignekv = ligne.split(KV.SEPARATOR);
				record = new KV(lignekv[0],lignekv[1]);
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
			String nligne = record.k + KV.SEPARATOR + record.v;
			bWriter.write(nligne +"\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.index++;
	}


}
