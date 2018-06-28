package cassandra;

public class Handler {

	private int rigID;
	private int manifoldID;
	private int wellID;
	private int treeID;
	private int sensorID;


	public Handler(int rigID, int manifoldID, int wellID, int treeID, int sensorID) {
		this.rigID = rigID;
		this.manifoldID = manifoldID;
		this.wellID = wellID;
		this.treeID = treeID;
		this.sensorID = sensorID;

	}


	public int getRigID() {
		return rigID;
	}


	public void setRigID(int rigID) {
		this.rigID = rigID;
	}


	public void setManifoldID(int manifoldID) {
		this.manifoldID = manifoldID;
	}


	public int getManifoldID() {
		return manifoldID;
	}


	public int getWellID() {
		return wellID;
	}


	public void setWellID(int wellID) {
		this.wellID = wellID;
	}
	
	public int getSensorID() {
		return sensorID;
	}

	public void setSensorID(int sensorID) {
		this.sensorID = sensorID;
	}

	public String toString() {
		return(String.format("%s, %s, %s, %s, %s", rigID, manifoldID, wellID, treeID, sensorID));
	}
}