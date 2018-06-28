package cassandra;

import java.sql.Date;

public class DataHandler {
	private Date date;
	private Double data;

	public DataHandler(Date date, Double data) {
		this.date = date;
		this.data = data;
	}
	
	public double getData() {
		return data;
	}

	public void setData(double data) {
		this.data = data;
	}
	public String toString() {
		return(String.format("%s, %f", date, data));
	}
}
