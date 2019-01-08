import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import java.io.IOException;

public class HBaseUtility {

	public static Configuration configuration;
	public static Connection connection;
	public static Admin admin;
	
	// create connection
	public static void init() {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
		try {
			connection = ConnectionFactory.createConnection(configuration);
			admin = connection.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// close connection
	public static void close() {
		try {
			if (admin != null) {
				admin.close();
			}
			if (null != connection) {
				connection.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * create table
	 * @param myTableName table name
	 * @param colFamily column family name
	 * @throws IOException
	 */
	public static void createTable(String myTableName, String[] colFamily) throws IOException {
		init();
		TableName tableName = TableName.valueOf(myTableName);
		
		if (admin.tableExists(tableName)) {  // table has existed
			System.out.println("Table is exists!");
		} else {
			HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
			for (String str:colFamily) {
				HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
				hTableDescriptor.addFamily(hColumnDescriptor);
			}
			admin.createTable(hTableDescriptor);
			System.out.println("Create table success");
		}
		close();
	}
	
	/**
	 * delete table
	 * @param tableName
	 * @throws IOException
	 */
	public static void deleteTable(String tableName) throws IOException {
		init();
		TableName tn = TableName.valueOf(tableName);
		if (admin.tableExists(tn)) {
			admin.disableTable(tn);
			admin.deleteTable(tn);
		}
		close();
	}
	
	/**
	 * list exists tables
	 * @throws IOException
	 */
	public static void listTables() throws IOException {
		init();
		HTableDescriptor hTableDescriptors[] = admin.listTables();
		for (HTableDescriptor hTableDescriptor:hTableDescriptors) {  // for every table in the list
			System.out.println(hTableDescriptor.getNameAsString());
		}
		close();
	}
	
	/**
	 * insert data into a column of a row
	 * @param tableName table name
	 * @param rowKey row key
	 * @param colFamily column family name
	 * @param col column name
	 * @param val value
	 * @throws IOException
	 */
	public static void insertRow(String tableName, String rowKey, String colFamily, String col, String val) throws IOException {
		init();
		Table table = connection.getTable(TableName.valueOf(tableName));
		Put put = new Put(rowKey.getBytes());
		put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
		table.put(put);
		table.close();
		close();
	}
	
	/**
	 * delete data
	 * @param tableName table name
	 * @param rowKey row key
	 * @param colFamily column family name
	 * @param col column name
	 * @throws IOException
	 */
	public static void deleteRow(String tableName, String rowKey, String colFamily, String col) throws IOException {
		init();
		Table table = connection.getTable(TableName.valueOf(tableName));
		Delete delete = new Delete(rowKey.getBytes());
		// delete all data from specified column family
		delete.addFamily(colFamily.getBytes());
		table.delete(delete);
		table.close();
		close();
	}
	
	/**
	 * get data by row key
	 * @param tableName table name
	 * @param rowKey row key
	 * @param colFamily column family name
	 * @param col column name
	 * @throws IOException
	 */
	public static void getData(String tableName, String rowKey, String colFamily, String col) throws IOException {
		init();
		Table table = connection.getTable(TableName.valueOf(tableName));
		Get get = new Get(rowKey.getBytes());
		get.addColumn(colFamily.getBytes(), col.getBytes());
		Result result = table.get(get);
		showCell(result);
		table.close();
		close();
	}
	
	/**
	 * formatted output
	 * @param result
	 */
	public static void showCell(Result result) {
		Cell[] cells = result.rawCells();
		for (Cell cell:cells) {
			System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
			System.out.println("Timestamp:" + cell.getTimestamp() + " ");
			System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
			// System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
			System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
		}
	}
}
