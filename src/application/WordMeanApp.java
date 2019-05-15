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

public class WordMeanApp implements MapReduce {
	private static final long serialVersionUID = 1L;

	// MapReduce program that computes word counts
	@Override
	public void map(FormatReader reader, FormatWriter writer) {

		Map<String,Integer> hm = new HashMap<>();
		KV kv;
		int i =0;
		while ((kv = reader.read()) != null) {
			StringTokenizer st = new StringTokenizer(kv.v);
			while (st.hasMoreTokens()) {
				String tok = st.nextToken();
				if (hm.containsKey(tok)) {
					i = i+1;
					hm.put(tok + Integer.toString(i), tok.length());  // On rajoute le i pour prendre en compte l'occurence des mots
				} else {
					hm.put(tok , tok.length());
				}
			}
		}
		for (String k : hm.keySet()) writer.write(new KV(k,hm.get(k).toString()));
	}

	@Override
	public void reduce(FormatReader reader, FormatWriter writer) {

        int sommeValeurs = 0;
        int nbValeurs = 0;
        KV kv;
		while ((kv = reader.read()) != null) {
			sommeValeurs += Integer.parseInt(kv.v);
			nbValeurs++;
		}
		int avg = Math.round((float) sommeValeurs/nbValeurs);
		writer.write(new KV("Average Words Length", Integer.toString(avg)));
	}

	public static void main(String args[]) {
		Job j = new Job();
        j.setInputFormat(Format.Type.LINE);
        j.setInputFname(args[0]);
       long t1 = System.currentTimeMillis();
		j.startJob(new WordMeanApp());
		long t2 = System.currentTimeMillis();
        System.out.println("time in ms ="+(t2-t1));
        System.exit(0);
		}
}
