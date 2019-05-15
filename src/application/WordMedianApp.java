package application;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.Job;

public class WordMedianApp implements MapReduce {
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

	class ValueComparator implements Comparator<String> {
	    Map<String, String> base;

	    public ValueComparator(Map<String, String> base) {
	        this.base = base;
	    }

	    // Note: this comparator imposes orderings that are inconsistent with
	    // equals.
	    @Override
		public int compare(String a, String b) {
	        if (base.get(a).compareTo(base.get(b)) <= 0) {
	            return -1;
	        } else {
	            return 1;
	        } // returning 0 would merge keys
	    }
	}

	@Override
	public void reduce(FormatReader reader, FormatWriter writer) {
		HashMap<String, String> hm = new HashMap<String, String>();
        ValueComparator bvc = new ValueComparator(hm);
        TreeMap<String, String> sorted_map = new TreeMap<String, String>(bvc);

        int nbValeurs = 0;
        KV kv;
		while ((kv = reader.read()) != null) {
				hm.put(kv.k, kv.v);
				nbValeurs++;

		}
		sorted_map.putAll(hm);
		int median_index = Math.round((float) nbValeurs/2);

		int i = 1;
		for (String k : sorted_map.keySet()) {
			if (i == median_index) {
				writer.write(new KV("Median Words Length", hm.get(k)));
				break;
			}
			i++;
		}
	}

	public static void main(String args[]) {
		Job j = new Job();
        j.setInputFormat(Format.Type.LINE);
        j.setInputFname(args[0]);
       long t1 = System.currentTimeMillis();
		j.startJob(new WordMedianApp());
		long t2 = System.currentTimeMillis();
        System.out.println("time in ms ="+(t2-t1));
        System.exit(0);
		}
}
