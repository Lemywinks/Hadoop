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

public class MonteCarloApp implements  MapReduce{
	
	private static final long serialVersionUID = 1L;

	public boolean isInRegion(String point) {
		String[] coordonnes = point.split("@");
		double x = new Double(coordonnes[0]);
		double y = new Double(coordonnes[1]);
		
		if (x*x + y*y <= 0.25) {
			return true;
		}
		return false;
	}
	
	
	public void map(FormatReader reader, FormatWriter writer) {
		
		Map<String,Integer> hm = new HashMap<>();
		KV kv;
		
		while ((kv = reader.read()) != null) {
			StringTokenizer st = new StringTokenizer(kv.v);
			while (st.hasMoreTokens()) {
				String tok = st.nextToken();
				if (isInRegion(tok)) {
					hm.put(tok, 1);
				} else {
					hm.put(tok, 0);
				}
			}
		}
		for (String k : hm.keySet()) writer.write(new KV(k,hm.get(k).toString()));
	}
	
	public void reduce(FormatReader reader, FormatWriter writer) {
        	
		KV kv;
		int ValeursInRegion = 0;
		int nbPoints = 0;
		while ((kv = reader.read()) != null) {
			
			if (Integer.parseInt(kv.v) == 1) {
				
				ValeursInRegion += 1;
			}
			nbPoints += 1;
		}
		float pi = (float) (4.0 * ValeursInRegion/nbPoints);
		writer.write(new KV("Pi estimation",Float.toString(pi)));
	}
	
	public static void main(String args[]) {
		Job j = new Job();
        j.setInputFormat(Format.Type.LINE);
        j.setInputFname(args[0]);
        j.setNumberOfReduces(1);
       long t1 = System.currentTimeMillis();
		j.startJob(new MonteCarloApp());
		long t2 = System.currentTimeMillis();
        System.out.println("time in ms ="+(t2-t1));
        System.exit(0);
	}
	
}
