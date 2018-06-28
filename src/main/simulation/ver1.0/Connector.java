package cassandra;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Connector {
	String defaultAdress="127.0.0.1";
	
	Cluster cluster;
	Session session;
	
	PreparedStatement insertSensor;
	PreparedStatement insertReading;
	
	PrintWriter readingFile;
	PrintWriter sensorFile;
	boolean writeToFile=false;
	
	public Connector(boolean use_txt) {
		if (use_txt==true){
			createFile();
			writeToFile=true;
		} else {
			this.cassandraConnect(defaultAdress);
			this.createTables();
		}
	}	
	public Connector() {
		this.cassandraConnect(defaultAdress);
		this.createTables();
	}	
	public Connector(String nodeAdress) {
		this.cassandraConnect(nodeAdress);
		this.createTables();
	}
	
	private void cassandraConnect(String nodeAdress) {
		String contactPoint=nodeAdress;
		System.out.println("Java connecting to Cassandra on " + contactPoint);
		cluster= Cluster.builder().addContactPoints(contactPoint).build();
		session= cluster.connect();
		System.out.println("Java connection to Cassandra established");
	}
	
	private void createFile() {
		System.out.println("creating simulation data text file");
		try {
			String readingFileName= "simulationDataNoDB_reading.txt";
			String sensorFileName= "simulationDataNoDB_sensor.txt"; 
			readingFile= new PrintWriter(readingFileName,"UTF-8");
			sensorFile= new PrintWriter(sensorFileName,"UTF-8");
			String outputString= String.format("simulation data files created named %s and %s", readingFileName, sensorFileName);
			System.out.println(outputString);
		} catch (FileNotFoundException e) {
			System.err.println("file not found for no DB data");
		} catch (UnsupportedEncodingException e) {
			System.err.println("encoding error while creating file for no DB");
		}
		
	}
	
	private void dropTables() {
		System.out.println("clearing Cassandra simulation setup");
		ArrayList<String> queries= new ArrayList<String>();
		queries.add("DROP KEYSPACE simulation;");
		for (String query : queries) {
			session.execute(query);
		}
	}
	
	private void truncateTables() {
		System.out.println("clearing Cassandra simulation Data");
		ArrayList<String> queries= new ArrayList<String>();
		queries.add("TRUNCATE simulation.reading;");
		queries.add("TRUNCATE simulation.sensor;");
		for (String query : queries) {
			session.execute(query);
		}
		
	}
	
	private void createTables() {
		System.out.println("setting up cassandra for simulation...");
		ArrayList<String> queries= new ArrayList<String>();
		queries.add("CREATE KEYSPACE IF NOT EXISTS simulation WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':'1'};");
		queries.add("USE simulation;");
		queries.add("CREATE TABLE IF NOT EXISTS sensor(rigID int, foldID int, treeID int, wellID int, usID int, quantity text, unit text, PRIMARY KEY((rigID), foldID, treeID, wellID, usID));");
		queries.add("CREATE TABLE IF NOT EXISTS reading(rigID int, foldID int, treeID int, wellID int, usID int, time timestamp, value float, PRIMARY KEY((rigID, foldID, treeID, wellID, usID), time));");
//		queries.add("TRUNCATE sensor;");
//		queries.add("TRUNCATE reading;");
		for (String query : queries) {
			System.out.println("attempting query");
			session.execute(query);
		}
		insertSensor= session.prepare("INSERT INTO sensor (rigID, foldID, treeID, wellID, usID, quantity, unit) VALUES (?,?,?,?,?,?,?);");
		insertReading= session.prepare("INSERT INTO reading (rigID, foldID, treeID, wellID, usID, time, value) VALUES (?,?,?,?,?,?,?);");
		System.out.println("end of cassandra setup");
	}
	
	public void emptyData() {
		truncateTables();
	}
	
	public void resetData() {
		dropTables();
		createTables();
	}
	
	public void addSensor(int[] sensorID, String quantity, String unit) {
		if (writeToFile==true) {
			sensorFile.println(String.format("%s,%s,%s,%s,%s,%s,%s", sensorID[0],sensorID[1], sensorID[2], sensorID[3], sensorID[4], quantity, unit));
			return;
		}
		session.execute(insertSensor.bind(sensorID[0], sensorID[1], sensorID[2], sensorID[3], sensorID[4], quantity, unit));
	}
	
	public void addReading(int[] sensorID, long time, double valueIn) {
		Date dateTime= new Date(time);
		addReading(sensorID, dateTime, valueIn);
	}
	
	public void addReading(int[] sensorID, Date time, double valueIn) {
		if (writeToFile==true) {
			readingFile.println(String.format("%s,%s,%s,%s,%s,%s,%s", sensorID[0], sensorID[1], sensorID[2], sensorID[3], sensorID[4], time.toString(), String.valueOf(valueIn)));
			return;
		}
		float value=(float)valueIn;
		if (sensorID.length!=5) {
			String temp= new String();
			for (int i : sensorID) {
				temp= temp+ ','+ Integer.toString(i);
			}
			System.err.println(String.format("sensorID %s is invalid", temp));
		} else {
			BoundStatement query= insertReading.bind(sensorID[0], sensorID[1], sensorID[2], sensorID[3], sensorID[4], time, value);
			session.execute(query);
		}
	}
	
	public void addReading(int[] sensorID, double value) {
		session.execute(insertReading.bind(sensorID[0], sensorID[1], sensorID[2], sensorID[3], sensorID[4], "dateof(now())", value));
	}
	
	public void endOfSimulation() {
		if (writeToFile==false) {
			return;
		}
		readingFile.close();
		sensorFile.close();
		writeToFile=false;
	}

	public static void main(String[] args) {
		Connector connector= new Connector();
		
	}
}
