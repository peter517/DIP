package tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import model.GlobalVars;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;


public class HbaseTableTools {
	
	/**
	 * create a table
	 */
	public static void creatTable(String tablename, String[] familyArr) throws Exception {

		HBaseAdmin admin = new HBaseAdmin(GlobalVars.getHbConf());
		if (admin.tableExists(tablename)) {
			dropTable(tablename);
		} 

			HTableDescriptor tableDesc = new HTableDescriptor(tablename);
			//columns could be added when addData
			for(int i = 0; i < familyArr.length; i++){
				tableDesc.addFamily(new HColumnDescriptor(familyArr[i]));
			}
			admin.createTable(tableDesc);
			System.out.println("create table ok .");
	}

	/**
	 * delete a table
	 * 
	 * @throws IOException
	 */
	public static void dropTable(String tablename) throws IOException {
		
		HBaseAdmin admin = new HBaseAdmin(GlobalVars.getHbConf());
		
		//firstly disable
		admin.disableTable(tablename);
		admin.deleteTable(tablename);
		System.out.println("drop table ok.");
		
	}
	
	/**
	 * addData by KeyValue
	 */
	public static void addKeyValue(String tablename, List<Put> putList) throws Exception {

		HTable table = new HTable(GlobalVars.getHbConf(), tablename);
		table.put(putList);
//		System.out.println("add data ok .");
		
	}

	/**
	 *  deleteData by rowKey
	 */
	public static void deleteKeyValue(String tablename, String rowKey) throws Exception {

		HTable table = new HTable(GlobalVars.getHbConf(), tablename);
		Delete d = new Delete(rowKey.getBytes());
//		d.deleteFamily(row.getBytes()); delete a family
//		d.deleteColumns(family, qualifier);delete a column
		table.delete(d);
		System.out.println("delete data ok .");
	}
	/** 
     * get KeyValue by rowKey 
     * @param tablename 
     * @param rowKey 
     * @throws IOException 
     */  
	public static List<KeyValue> getKeyValue(String tablename, String rowKey) throws IOException {

		HTable table = new HTable(GlobalVars.getHbConf(), tablename);
		Get g = new Get(rowKey.getBytes());
		List<KeyValue> kvList = new ArrayList<KeyValue>();
		
		Result result = table.get(g);
		for (KeyValue kv : result.raw()) {
			kvList.add(kv);
//        	System.out.println(new String(kv.getRow()) + "\t" 
//       			 + new String(kv.getFamily()) + "\t" 
//       			 + new String(kv.getQualifier()) + "\t" // get column
//       			 +new String(kv.getValue()));
        }  
			return kvList;
        
    }  
	/**
	 * displayAllKeyValue 	
	 */
	public static void displayAllKeyValue(String tablename) throws Exception {

		HTable table = new HTable(GlobalVars.getHbConf(), tablename);
		Scan s = new Scan();
		ResultScanner rs = table.getScanner(s);

		for (Result r : rs) {
			for (KeyValue kv : r.raw()) {
				System.out.println(new String(kv.getRow()) + "\t" 
			 + new String(kv.getFamily()) + "\t" 
			 + new String(kv.getQualifier()) + "\t" // get column
			 +new String(kv.getValue()));
			}

		}
	}
}
