package config;

import java.util.HashMap;

public class Project {

	public static String PATH = "";
	public static String PATHF = "";


	public static HashMap<String, Integer> lServeurs = new HashMap<>();
	static {
		lServeurs.put("Server4", 2225);
		lServeurs.put("Server5", 2226);
	}

}
