package cassandra;

import java.util.Map;

public class Reservoir extends Section{

	public Reservoir(int[] sectionID, long randomSeed, Controller master, Connector cassandraConnector,	Map<String, Double> values, Map<Integer, String> sensors, double dt) {
		super(sectionID, randomSeed, master, cassandraConnector, values, sensors, dt);
		this.calcIin();
	}
	
	public void run() {
		
	}
	

}
