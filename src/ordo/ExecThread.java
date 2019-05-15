package ordo;

import formats.Format;

public class ExecThread extends Thread{

	private Daemon daemon;
	private CallBack cb;
	private Format writer;
	private Format reader;

	public ExecThread(Daemon daemon, Format reader, Format writer, CallBack cb) {
		this.daemon = daemon;
		this.reader = reader;
		this.writer = writer;
		this.cb = cb;
	}


}
