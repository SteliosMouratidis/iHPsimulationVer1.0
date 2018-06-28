package cassandra;

import java.security.SecureRandom;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

//import cassandra.cassandraConnector.Connector;

public class Controller extends Thread{
	// helper objects
	Connector cassandra;
	SecureRandom seedGen;
	
	// for simulation
	ArrayList<Section> sections;
	ArrayList<Reservoir> reservoirs;
	boolean keepGoing=false; // used to stop the simulation
	long currentTime; //simulation time // (s)
	double dt; // timestep // (s)
	double readInterval; // how often do you write Data to Cassandra // (s)
	long maximumTime;
	HashMap<String, String[]> quants; // maps from shorthand to description and unit e.g. 'p'-->['pressure', 'Pa']
	
	public Controller() {
		cassandra= new Connector(true); // now in text file mode!!
		//cassandra.resetData();
		seedGen= new SecureRandom();
		sections= new ArrayList<Section>();
		reservoirs= new ArrayList<Reservoir>();
		currentTime=0;
		dt=1; //(seconds)
		readInterval=1; // (seconds)
		maximumTime=100; //(seconds)
//		maximumTime=1*24*60*60;
		
		quants= new HashMap<String, String[]>();
		String[] helper= new String[] {"pressure","Pa"};
		quants.put("P", helper);
		helper= new String[] {"temperature","K"};
		quants.put("T", helper);
		helper= new String[] {"flow","m/s"};
		quants.put("flow", helper);
		
	}
	
	public void test() {
		int[] treeID= new int[] {1,1,1};
		setupTree(treeID,3);
		for (int i=0; i<this.sections.size(); i++) {
			Section sec= this.sections.get(i);
			if (sec.getSectionID()[3]!=0) {
				makeReservoir(i);
			} else {
				if (sec.getSectionID()[2]!=1) {
					// TODO set up tank, necessary once Iout is influenced by next section
				}
			}
		}		
	}
	
	public void setupTree(int[] treeID, int n) {
		// sets up a tree with specified number of wells
		// sets up tree section as next section for all wells
		// does not set up a next section for the tree
		int[] sectionID= new int[4];
		for (int i=0; i<treeID.length; i++) {
			sectionID[i]=treeID[i];
		} // create sections
		int index= sections.size(); // index of the tree
		for (int i=0; i<=n; i++) {
			sectionID[3]=i;
			makeSection(sectionID);
		} // register previous/next sections between tree and wells
		Section tree=sections.get(index);
		Section sec;
		for (int i=index+1; i<sections.size(); i++) {
			sec= sections.get(i);
			sec.defineNextSection(tree);
			tree.addToPreviousSections(sec);
		}
		// noTODO don't need tank anymore
	}
	
	private void makeSection(int[] sectionID) {
		// creates one section with default physical values and sectionID as specified
		// sets up the default sensors with their IDs and writes to DB
		// does not handle previous or next section set up
		System.out.println("setting up test section");
		HashMap<String, Double> map= new HashMap<String, Double>();
		HashMap<Integer,String> sensors= new HashMap<Integer,String>();
		map.put("l", (double)100); // pipe length
		map.put("d", 0.2); // pipe diameter
		map.put("P", (double)7000000); // pressure
		sensors.put(0, "P");
		map.put("T", (double)277); // temperature
		sensors.put(1,"T");
		map.put("flow", (double)10); // flow
		sensors.put(2, "flow");
		map.put("density", (double)60);
		map.put("sensorNoise",(double)0.1);
		map.put("physNoise",(double)0.0);
		map.put("Imax",(double)100);
		map.put("maxFlow",(double)100);
		long seed= seedGen.nextLong();
		sections.add(new Section(sectionID, seed, this, cassandra, map, sensors, dt));
		registerSensors(sectionID, sensors);
	}
	
	private void makeReservoir(int index){
		// creates a reservoir (section that does not change much over time, providing a constant flow into a section that represents a well)
		// registers this reservoir with the section at specified index in sections list
		System.out.println("setting up test reservoir");
		HashMap<String, Double> map= new HashMap<String, Double>();
		HashMap<Integer,String> sensors= new HashMap<Integer,String>();
		map.put("l", (double)100); // pipe length
		map.put("d", 0.2); // pipe diameter
		map.put("P", (double)7000000); // pressure
		map.put("T", (double)277); // temperature
		map.put("flow", (double)10); // flow
		map.put("density", (double)60);
		map.put("sensorNoise",(double)0.1);
		map.put("physNoise",(double)0.0);
		map.put("Imax",(double)100);
		map.put("maxFlow",(double)100);
		long seed= seedGen.nextLong();
		int[] sectionID= new int[] {999999,999999,999999,999999}; // basically code for non valid ID
		Reservoir res= new Reservoir(sectionID, seed, this, cassandra, map, sensors, dt);
		Section sec= this.sections.get(index);
		sec.addToPreviousSections(res);
		res.defineNextSection(sec);
		this.reservoirs.add(res);
	}
	
	private void makeTank(int index) {
		// TODO
	}
	
	private void registerSensors(int[] sectionID, Map<Integer,String> sensors) {
		int[] sensorID= new int[5];
		for (int i=0; i<4; i++) {
			sensorID[i]=sectionID[i];
		}
		String key;
		String[] helper;
		for (int i : sensors.keySet()) {
			sensorID[4]=i;
			key= sensors.get(i);
			helper= quants.get(key);
			cassandra.addSensor(sensorID, helper[0], helper[1]);
		}
	}
	
	public void run() {
		System.out.println("initiating simulation");
		keepGoing=true;
		LocalDateTime formattedTime= LocalDateTime.of(0,1,1,0,0,0);	// used to output simulation time
		// simulation loop ---------------------------------------------
		while (keepGoing==true) {
			currentTime=currentTime + (long)(dt*1000); // timestep (long is in milliseconds)
			for (Section section : sections) { // trigger sections to calculate and regularly write to Cassandra, plus reseed
				section.run();
				if ((currentTime % (readInterval*1000))==0) { // Cassandra write
					section.readSensors();
					section.newSeed(seedGen.nextLong()); // reseed after every write
				}
			} // end of sections calculating
			
			if ((currentTime% (30*60*1000)) == 0) { // print message for every half hour of simulation time;
				formattedTime.plusMinutes(30);
				System.out.println(String.format("simulation time: %s days %s:%s:%s",formattedTime.getDayOfYear(), formattedTime.getHour(), formattedTime.getMinute(), formattedTime.getSecond()));
			}
			if (currentTime>(maximumTime*1000)) { // automatically finish after some time has passed;
				System.out.println("end of simulation time");
				break;
			}
		}
		// terminating simulation -----------------------------------------
		System.out.println("terminating simulation");
		cassandra.endOfSimulation();
		System.out.println("simulation terminated");
	}
	
	public long getTime() {
		return currentTime;
	}
	
	public void stopGoing() {
		keepGoing=false;
	}
	
	public static void main(String[] args) {
		Scanner reader= new Scanner(System.in);
		System.out.println("setting up simulation for iHP Data");
		Controller controller= new Controller();
		controller.test();
		System.out.println("starting simulation");
		controller.start();
		try {Thread.sleep(1);} catch (InterruptedException e) {	System.err.println("interrupt occured...");	}
		System.out.println("press Enter to stop simulation");
		reader.nextLine();
		controller.stopGoing();
		System.out.println("end of main");
		reader.close();
	}


}
