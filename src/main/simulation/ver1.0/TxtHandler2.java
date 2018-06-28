package cassandra;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class TxtHandler2 {
	public static void main(String[] args) {
		try {
			File f = new File(
					"C:\\Users\\t16sm17\\eclipse-workspace\\cassandra\\cassandra\\simulationDataNoDB_reading.txt");
			@SuppressWarnings("resource")
			Scanner sc = new Scanner(f);
			HashMap<Handler, DataHandler> map = new HashMap<Handler, DataHandler>();
			while (sc.hasNextLine()) {
				String line = sc.nextLine();
				String[] raw = line.split(",");
				int rigID = Integer.parseInt(raw[0]);
				int manifoldID = Integer.parseInt(raw[1]);
				int treeID = Integer.parseInt(raw[2]);
				int wellID = Integer.parseInt(raw[3]);
				int sensorID = Integer.parseInt(raw[4]);
				Date date = Date.valueOf(raw[5]);
				double data = Double.parseDouble(raw[6]);
				Handler p = new Handler(rigID, manifoldID, treeID, wellID, sensorID);
				DataHandler d = new DataHandler(date, data);
				map.put(p, d);
			}
			// System.out.println(Collections.singletonList(map));
			for (Map.Entry<Handler, DataHandler> entry : map.entrySet()) {
				System.out.println(entry.getKey() + " : " + entry.getValue());
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
}
