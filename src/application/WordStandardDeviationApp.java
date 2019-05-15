package application;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.Job;

public class WordStandardDeviationApp implements MapReduce {
	private static final long serialVersionUID = 1L;

	// MapReduce program that computes word counts
	public void map(FormatReader reader, FormatWriter writer) {
		
		Map<String,Integer> hm = new HashMap<>();
		KV kv;
		while ((kv = reader.read()) != null) {
			StringTokenizer st = new StringTokenizer(kv.v);
			while (st.hasMoreTokens()) {
				String tok = st.nextToken();
				hm.put(tok, tok.length());
			}
		}
		for (String k : hm.keySet()) writer.write(new KV(k,hm.get(k).toString()));
	}
	
	public void reduce(FormatReader reader, FormatWriter writer) {
		Map<String,String> hm = new HashMap<>();
		
        int sommeValeurs = 0;
        int nbValeurs = 0;
        KV kv;
		while ((kv = reader.read()) != null) {
			sommeValeurs += Integer.parseInt(kv.v);
			nbValeurs++;	
			hm.put(kv.k, kv.v);
		}
		float avg = (float) sommeValeurs/nbValeurs;
		float variance = 0;
		for (String k : hm.keySet()) {
			float f = (float) Integer.parseInt(hm.get(k));			
			variance += Math.pow((f - avg),2);				
		}
		variance = variance/nbValeurs;
		float deviation = (float) Math.sqrt(variance);
		writer.write(new KV("Standard Deviation Words Length", Float.toString(deviation)));
	}
	
	public static void main(String args[]) {
		Job j = new Job();
        j.setInputFormat(Format.Type.LINE);
        j.setInputFname(args[0]);
       long t1 = System.currentTimeMillis();
		j.startJob(new WordStandardDeviationApp());
		long t2 = System.currentTimeMillis();
        System.out.println("time in ms ="+(t2-t1));
        System.exit(0);
		}
}
