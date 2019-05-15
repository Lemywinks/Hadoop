package application;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.Job;

public class WordModeApp implements MapReduce {
	private static final long serialVersionUID = 1L;

	// MapReduce program that computes word counts
	public void map(FormatReader reader, FormatWriter writer) {
		
		Map<String,Integer> hm = new HashMap<>();
		KV kv;
		while ((kv = reader.read()) != null) {
			StringTokenizer st = new StringTokenizer(kv.v);
			while (st.hasMoreTokens()) {
				String tok = st.nextToken();
				if (hm.containsKey(tok)) hm.put(tok, hm.get(tok).intValue()+1);
				else hm.put(tok, 1);
			}
		}
		for (String k : hm.keySet()) writer.write(new KV(k,hm.get(k).toString()));
	}
	
	
	public void reduce(FormatReader reader, FormatWriter writer) {		
        KV kv;
        int max = 0;
        List<String> l = new ArrayList<String>();
		while ((kv = reader.read()) != null) {
				if (Integer.parseInt(kv.v) > max) {
					max = Integer.parseInt(kv.v);
					l.clear();
					l.add(kv.k);
				} else if (Integer.parseInt(kv.v) == max) {
					l.add(kv.k);
				}
								
		}
		writer.write(new KV("Max of words repetition", Integer.toString(max)));
		writer.write(new KV("Mode Words", l.toString()));					
	}
	
	public static void main(String args[]) {
		Job j = new Job();
        j.setInputFormat(Format.Type.LINE);
        j.setInputFname(args[0]);
       long t1 = System.currentTimeMillis();
		j.startJob(new WordModeApp());
		long t2 = System.currentTimeMillis();
        System.out.println("time in ms ="+(t2-t1));
        System.exit(0);
		}
}
