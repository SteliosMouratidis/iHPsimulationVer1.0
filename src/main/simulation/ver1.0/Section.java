package cassandra;

import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

//import cassandra.cassandraConnector.Connector;

public class Section {
	//	helpers
	protected Random random; // class to create gaussian noise for sensors
	protected Connector cassandra; // connects to DB to store (sensor-)Data
	protected Controller master; // controller that supervises the entire simulation and all sections in it
	int Imax;

	// 	physical variables
	protected double dt; // timestep size (seconds
	protected double physNoise;
	protected HashMap<String,Double> physicalValues; // current values and some default/ normal values (mostly for future improvements
	protected double k=0;

	//	structural variables
	protected int[] sectionID; // ID for this section i.e. rigID, foldID, treeID, wellID
	protected double sensorNoise;
	protected HashMap<Integer,String> sensors; // list of sensors with their usID and a tag of what they measure
	protected List<Section> previousSections; // the sections feeding into this section
	protected Section nextSection; // the section this section is feeding into

	//	Constructor****************************************************************************************
	public Section(int[] sectionID, long randomSeed, Controller master, Connector cassandraConnector, Map<String, Double> values, Map<Integer,String> sensors, double dt) { 
		// expected format for values: {<l:[value]>, <d:[value]>, <P:[current, natural, default]>, <T[current, natural]>, <Iout:[current]>, <Iin:[current, default]>, <m:[null]>, <flow:[null]>, <density:[current]>}
		// P: pressure ; T: temperature ; Iout: mass flow rate out of the pipe ; Iin: mass flow rate into the pipe ; m: mass of "fluid" in the pipe ; flow: flow speed out of the pipe ; l: length of pipe ; d: diameter of pipe

		previousSections= new ArrayList<Section>();
		this.dt=dt;
		this.sectionID= new int[4];
		for (int i=0; i<4; i++) {
			this.sectionID[i]=sectionID[i];
		}
		random= new Random(randomSeed);
		cassandra= cassandraConnector;
		this.master= master;
		this.sensors= new HashMap<Integer,String>(sensors);
		this.physicalValues= new HashMap<String, Double>(values);
		double l= this.getValue("l");
		double d=this.getValue("d");
		double A= Math.PI * Math.pow((d/2),2);
		this.alterValues("A", A);
		double Aout= this.getValue("A");
		this.alterValues("Aout", Aout);
		double V=A*l;
		this.alterValues("V", V);
		double ht=20;
		this.alterValues("ht", ht);
		this.sensorNoise=this.getValue("sensorNoise");
		this.physNoise=this.getValue("physNoise");
		this.Imax= (int)this.getValue("Imax");
		physicalValues.remove("Imax");	
		physicalValues.remove("sensorNoise");
		physicalValues.remove("physNoise");		
		physicalValues.putIfAbsent("m", this.getValue("density")*this.getValue("V"));
		physicalValues.putIfAbsent("Iin", this.getValue("flow")*this.getValue("density")*this.getValue("A"));
		physicalValues.putIfAbsent("Iout", this.getValue("flow")*this.getValue("density")*this.getValue("A"));
		physicalValues.put("lastP", 0.0);
		//this.calcPT();this.calcPT();
		physicalValues.put("lastIin", 0.0);
		this.calcIin();this.calcIin();
		physicalValues.put("H", 0.0);
		for (String tag: sensors.values()) {
			physicalValues.put(tag+"Sensor", this.getValue(tag));
		}

		this.debugPrintValues();
	}

	//	public functions *********************************************************************************


	// read Sensor values into DB, uses physical value the sensor should read and adds noise on top
	public void readSensors() { // override with default noise of 0.1
		String tag;
		double value;
		long time= (long)master.getTime();
		//		Date time= new Date(master.getTime());
		int[] sensorID= new int[5];
		for (int i=0; i<sectionID.length; i++) {
			sensorID[i]=sectionID[i];
		}
		for (int key : sensors.keySet()) {
			sensorID[sensorID.length-1]=key;
			tag= sensors.get(key);
			value= this.getValue(tag+"Sensor");
			value= normal(value, sensorNoise*value);
			cassandra.addReading(sensorID,  time, value);
		}
	}
	
	public int[] getSectionID() {
		return this.sectionID;
	}

	// universal getter
	public double getValue(String key) {
		double value= physicalValues.get(key);
		//System.out.println(String.format("%s:%s", key,Double.toString(value)));
		return value;
	}

	// universal setter
	public void alterValues(String key, double value) {
		//System.out.println(String.format("%s:%s", key,Double.toString(value)));
		this.physicalValues.put(key, value);
	}
	public void alterValues(String key, int index, double value) {
		this.alterValues(key, value);
		/*
		double[] prop;
		prop= physicalValues.get(key);
		prop[index]=(double)value;
		physicalValues.put(key,  prop);
		 */
	}

	// relates this section to those that feed into it
	public void addToPreviousSections(Section addedSection) {
		previousSections.add(addedSection);
		this.calcIin();this.calcIin();
	}	
	// relates this section to those that feed into it
	public void addToPreviousSections(List<Section> addedSections) {
		for (Section section : addedSections) {
			previousSections.add(section);
		} 
		this.calcIin();this.calcIin();
	}

	// relates this section to the one it is feeding into
	public void defineNextSection(Section section) {
		nextSection= section;
	}

	// reset the seed for creating Gaussian noise, required to make noise truly unpredictable
	public void newSeed(long seed) {
		random.setSeed(seed);
	}

	//	advance this section by one dt in the simulation
	public void run() {
		this.debugPrintValues();///////////////////////////////////
		calcIin();
		calcVout();
		calcIout();
		calcM();
		calcPT();
		calcDensity();
		calcH();
		calcSensors();
	}	
	
	// TODO public void alterIout(???){ adjust Iout due to blockage in next section } 
	// must take different weightings of previous sections into account
	// use k= excess mass in section. 
	// this mass could be reduced in the previous sections from Iout in the next timestep, based on their relative weighting
	// 
	
	public void setK(double k) {
		this.k=k;
	}

	//	private Maths functions****************************************************************************
	// TODO hydrates <=> MEG
	
	// mass flow rate into the section 'Iin', from the Iout of sections feeding into this one
	protected void calcIin() {
		double I= this.getValue("Iin");
		this.alterValues("lastIin", 0, I);
		I=0;
		double value;
		for (Section section : this.previousSections) {
			value= section.getValue("Iout");
			I= I+ value;
		} this.alterValues("Iin", 0, I);
	}
	
	// flow speed out of the Section, has a limit
	protected void calcVout() {
		double A= this.getValue("Aout");
		double v;
		if (A!=0) {
			double p= this.getValue("density");
			double Vmax= this.getValue("maxFlow");
			double I= this.getValue("Iin");
			v= (I)/(p*A);
			v= v-Vmax;
			I= I- (p*A*Vmax);
			I= Math.exp(I)+1;
			v= v/I;
			v= v+Vmax;
		}else {
			v=0;
		}
		this.alterValues("flow", 0, v);
	}
	
	// Mass flow out of the Section based on flow speed
	protected void calcIout() {
		double v= this.getValue("flow");
		double p= this.getValue("density");
		double A= this.getValue("Aout");
		double I= this.getValue("Iout");
		this.alterValues("lastIout", I);
		I= p*A*v;
		//I= I-k*v;
		//this.k=0;
		//v= I/(p*A);
		//this.alterValues("v",v);
		this.alterValues("Iout", 0, I);
	}

	// Runge Kutta of the Mass of Gas inside the pipe / dm/dt= Iin- Iout;
	protected void calcM(){
		double k;
		double m= this.getValue("m");
		double lastIout= this.getValue("lastIout");
		double Iout= this.getValue("Iout");
		double halfIout= (Iout+lastIout)/2;
		double lastIin= this.getValue("lastIin");
		double Iin= this.getValue("Iin");
		double halfIin= (lastIin+Iin)/2;
		k= 0;
		k= k+ (lastIin-lastIout);
		k= k+ 2*(halfIin-halfIout);
		k= k+ 2*(halfIin-halfIout);
		k= k+ (Iin-Iout);
		k= (dt/6)*k;
		// TODO if k>0 then previous sections based on weighting section.alterIout(k*weight)
		m= m+ k;
		this.alterValues("m",m);
		alterPrevious(k);
	}
	protected void alterPrevious(double k) {
		double I;
		double Iin= this.getValue("Iin");
		for (Section sec: this.previousSections) {
			I= sec.getValue("Iout");
			I= I/Iin;
			sec.setK(I*k);
		}
	}

	// pressure and temperature given the current mass
	// begins by calculating the volume this mass would have if it could expand freely => Cpw,Ctp 
	// then how pressure changes if it is compressed to the volume of the Section usign Cpw
	// then calculates Temperature based on that and Ctp
	protected void calcPT() {
		double m= this.getValue("m");
		double p= this.getValue("density");
		double T= this.getValue("T");
		double V= m/p;
		double y= 2.2/1.7;
		double P= this.getValue("P");
		double Cpw= P*Math.pow(V, y);
		double Ctp= (P*V)/T; 
		V= this.getValue("V");
		P= Cpw/Math.pow(V,y);
		T= (P*V)/Ctp;
		this.alterValues("P", P);
		this.alterValues("T", T);
	}
	
	// 'Density' (usually Greek 'rho' which looks like p) simply the mass per volume as both parameters are fied already
	protected void calcDensity() {
		// after mass (m)
		double V= this.getValue("V");
		double m= this.getValue("m");
		double p= m/V;
		this.alterValues("density", p);
	}
	// based on HCondition (whether H grows or shrinks) grows/shrinks H exponentially up to a limit
	// H=0: no hydrates ; H=1: plug i.e. pipe is fully blocked ; H>1: plug grows along the pipe making it harder to remove
	// then updates Aout 
	protected void calcH() {
		double d= this.getValue("d");
		double H= this.getValue("H");
		double ht= this.getValue("ht");
		double Aout;
		if (HCondition()==true) { // true=> hydrate growing
			if (H==0.0) {
				H=0.0001;
			} else {
				H= (dt/ht)*2*H;
			}
		} else { // false=> hydrate shrinking
			H= H/(2*(dt/ht));
		}
		if (H>3) { // can grow along pipe, but not too much
			H=3;
		}
		this.alterValues("H", H);
		//Aout
		if (H>1) {H=1;}
		Aout=Math.PI*Math.pow(((1-H)*d)/2,2);
		this.alterValues("Aout",Aout);
	}

	// assess whether hydrate should grow or not (relation of P,T and MEG(TODO)
	protected boolean HCondition() {
		double T= this.getValue("T");
		double P= this.getValue("P");
		return ( T < ((Math.pow(6894.7*P, 0.285)*8.9 + 459.67)*(5.0/9.0) ));
	}

	// create Gaussian noise
	protected double normal(double mean, double sigma) {
		return (mean + sigma* random.nextGaussian());
	}
	
	// keep sensor values up to date
	protected void calcSensors() {
		double value;
		double I= this.getValue("Iin");
		double p= this.getValue("density");
		double A= this.getValue("A");
		value= I/(p*A); // flow that is measured is not the flow through the hydrate=> use flow into
		this.alterValues("flowSensor", value);
		value= this.getValue("T");
		this.alterValues("TSensor", value);
		value= this.getValue("P");
		this.alterValues("PSensor", value);
		
	}
	
	protected double stepNoise(double totalK, double noise) {
		totalK= (dt/6)*totalK;
		noise= totalK*noise;
		totalK= this.normal(totalK,noise*totalK);
		return totalK;
	}

	//	protected debugging functions *********************************************************************
	protected void debugPrintValues() {
		System.out.println("\nprintin all values");
		long time= (long)(master.getTime());
		System.out.println(String.format("%s, %s%s%s%s",time, sectionID[0], sectionID[1], sectionID[2], sectionID[3]));
		for (String key : this.physicalValues.keySet()) {
			System.out.println(String.format("%s : %s", key,Double.toString(this.getValue(key))));
		}
		System.out.println(String.format("%s : %s", "Aout", this.getValue("Aout")));
	}
}
