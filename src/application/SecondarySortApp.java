package application;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.Job;

public class SecondarySortApp implements MapReduce {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	class ValueComparator implements Comparator<String> {
	    Map<String, ?> base;

	    public ValueComparator(Map<String, ?> base) {
	        this.base = base;
	    }

	    // Note: this comparator imposes orderings that are inconsistent with
	    // equals.
	    public int compare(String a, String b) {
	        if (a.compareTo(b) <= 0) {
	            return -1;
	        } else {
	            return 1;
	        } // returning 0 would merge keys
	    }
	}

	// MapReduce program that computes elements sorting
		public void map(FormatReader reader, FormatWriter writer) {
			HashMap<String, String> hm = new HashMap<String, String>();
	        ValueComparator bvc = new ValueComparator(hm);
	        TreeMap<String, String> sorted_map = new TreeMap<String, String>(bvc);

			KV kv;
			while ((kv = reader.read()) != null) {
				StringTokenizer st = new StringTokenizer(kv.v);
				if (st.hasMoreTokens()) {
					String tok = st.nextToken();
					if (st.hasMoreTokens()) {
						String tvalue = st.nextToken();
						if (hm.containsKey(tok)) { 
							hm.put(tok, hm.get(tok) + tvalue);
						} else { hm.put(tok, tvalue + ", "); }
					}
				}
			}
			sorted_map.putAll(hm);
			for (String k : sorted_map.keySet()) writer.write(new KV(k,hm.get(k)));
		}
		
		public void reduce(FormatReader reader, FormatWriter writer) {
	                Map<String,List<String>> hm = new HashMap<>();
	                ValueComparator bvc = new ValueComparator(hm);
	    	        TreeMap<String, List<String>> sorted_map = new TreeMap<String, List<String>>(bvc);
	                
	                
	        String [] ls;
	        
			KV kv;
			while ((kv = reader.read()) != null) {
					List<String> l = new ArrayList<String>();
					
					ls = kv.v.split(", ");
					for (String k : ls) l.add(k);

					Collections.sort(l);

					hm.put(kv.k, l);
				
			}
			sorted_map.putAll(hm);
			for (String k : sorted_map.keySet()) writer.write(new KV(k,hm.get(k).toString()));
		}
	public static void main(String args[]) {
		Job j = new Job();
        j.setInputFormat(Format.Type.LINE);
        j.setInputFname(args[0]);
       long t1 = System.currentTimeMillis();
		j.startJob(new SecondarySortApp());
		long t2 = System.currentTimeMillis();
        System.out.println("time in ms ="+(t2-t1));
        System.exit(0);
		}
}
