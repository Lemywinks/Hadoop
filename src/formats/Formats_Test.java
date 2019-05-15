package formats;


import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class Formats_Test {

	private KV record;
	private KVFormat f_KV;
	private LineFormat f_Line;


	@Before public void setUp() {
		f_KV = new KVFormat("Test_KV.txt");
		f_Line = new LineFormat("Test_Line.txt");
		record = new KV("1","Test");
	}

	@Test public void testerKV() {
		f_KV.write(record);
		KV res = f_KV.read();
		assertTrue(res.k.equals("1"));
		assertTrue(res.v.equals("Test"));
	}

	@Test public void testerLine() {
		f_Line.write(record);
		KV res = f_Line.read();
		assertTrue(res.k.equals("1"));
		assertTrue(res.v.equals("Test"));
	}


}
