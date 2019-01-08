import java.io.IOException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class Test {

	public static void main(String[] args) throws IOException {
		// create table
		HBaseUtility.createTable("CarComment", new String[]{"content", "subject", "sentiment_value", "sentiment_word"});
		
		// check table
		System.out.println("Current tables:");
		HBaseUtility.listTables();
		
		// insert data from local file
		File file = new File("src/train.txt");  // read file
		BufferedReader reader = null;
		String tmp;
		int line = 0;
		long startTime = System.currentTimeMillis();
		try {
			reader = new BufferedReader(new FileReader(file));
			while ((tmp = reader.readLine()) != null) {
				String[] arrayStr = tmp.split(",");
				HBaseUtility.insertRow("CarComment", arrayStr[0], "content", "", arrayStr[1]);
				HBaseUtility.insertRow("CarComment", arrayStr[0], "subject", "", arrayStr[2]);
				HBaseUtility.insertRow("CarComment", arrayStr[0], "sentiment_value", "", arrayStr[3]);
				if (arrayStr.length < 5) {
					HBaseUtility.insertRow("CarComment", arrayStr[0], "sentiment_word", "", "null");
				} else {
					HBaseUtility.insertRow("CarComment", arrayStr[0], "sentiment_word", "", arrayStr[4]);
				}
				System.out.println("Inserted 1 record...");
				line++;
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		long endTime = System.currentTimeMillis();
		System.out.println("insert " + line + " records in " + (endTime - startTime) + " ms");
		
		 // get the first 101 data by row key
		// row keys are read from local file
		line = 0;
		startTime = System.currentTimeMillis();
		try {
			reader = new BufferedReader(new FileReader(file));
			while ((tmp = reader.readLine()) != null && line <= 100) {
				String[] arrayStr = tmp.split(",");
				HBaseUtility.getData("CarComment", arrayStr[0], "content", "");
				HBaseUtility.getData("CarComment", arrayStr[0], "subject", "");
				HBaseUtility.getData("CarComment", arrayStr[0], "sentiment_value", "");
				HBaseUtility.getData("CarComment", arrayStr[0], "sentiment_word", "");
				line++;
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
		    }
		}
		endTime = System.currentTimeMillis();
		System.out.println("get " + line + " records in " + (endTime - startTime) + " ms");
		
		// delete a data by row key
		HBaseUtility.deleteRow("CarComment", "vUXizsqexyZVRdFH", "content", "");
		// search it for test
		HBaseUtility.getData("CarComment", "vUXizsqexyZVRdFH", "content", "");
		
		// test delete table
		HBaseUtility.createTable("NBA", new String[]{"name", "team"});
		System.out.println("Current tables:");
		HBaseUtility.listTables();
		HBaseUtility.deleteTable("NBA");
		System.out.println("After delete:");
		HBaseUtility.listTables();
		// HBaseUtility.deleteTable("CarComment");
	}
}
