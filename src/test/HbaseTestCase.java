package test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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

@SuppressWarnings("deprecation")
public class HbaseTestCase {

	private static Configuration conf = HBaseConfiguration.create();
	public HbaseTestCase() {
		// set conf
	}

	/**
	 * 创建一张表
	 */
	public static void creatTable(String tablename) throws Exception {

		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tablename)) {
			System.out.println("table   Exists!!!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tablename);
			//colunm could be added when addData
			tableDesc.addFamily(new HColumnDescriptor("n1"));
			tableDesc.addFamily(new HColumnDescriptor("n2"));
			admin.createTable(tableDesc);
			System.out.println("create table ok .");
		}

	}

	/**
	 * delete a table
	 * 
	 * @throws IOException
	 */
	public static void dropTable(String tablename) throws IOException {
		
		HBaseAdmin admin = new HBaseAdmin(conf);
		admin.disableTable(tablename);
		admin.deleteTable(tablename);
		System.out.println("drop table ok.");
		
	}

	/**
	 * 添加一条数据
	 */
	public static void addData(String tablename) throws Exception {

		HTable table = new HTable(conf, tablename);

		List<Put> putList = new ArrayList<Put>();
		for (int i = 0; i < 3; i++) {
			Put putrow = new Put(String.valueOf(i).getBytes());
			//family: n1, column: name2, value
			putrow.add("n1".getBytes(), "name2".getBytes(), String.valueOf("数据").getBytes());
			putList.add(putrow);
		}
		
		table.put(putList);
		System.out.println("add data ok .");
		
	}

	/**
	 * 添加一条数据
	 */
	public static void deleteData(String tablename, String row) throws Exception {

		HTable table = new HTable(conf, tablename);
		Delete d = new Delete(row.getBytes());
//		d.deleteFamily(row.getBytes()); delete a family
//		d.deleteColumns(family, qualifier);delete a column
		table.delete(d);
		System.out.println("delete data ok .");
	}
	/** 
     * get rowKey search 
     * @param tablename 
     * @param rowKey 
     * @throws IOException 
     */  
	public static void getByRowKey(String tablename, String rowKey) throws IOException {

		HTable table = new HTable(conf, tablename);
		Get g = new Get(rowKey.getBytes());
		Result r = table.get(g);
		for (KeyValue kv : r.raw()) {
        	System.out.println(new String(kv.getRow()) + "\t" 
       			 + new String(kv.getFamily()) + "\t" 
       			 + new String(kv.getQualifier()) + "\t" // get column
       			 +new String(kv.getValue()));
        }  
        
    }  
	/**
	 * 显示所有数据
	 */
	public static void getAllData(String tablename) throws Exception {

		HTable table = new HTable(conf, tablename);
		Scan s = new Scan();
		ResultScanner rs = table.getScanner(s);

		int c = 0;
		for (Result r : rs) {
			for (KeyValue kv : r.raw()) {
				c++;
//				System.out.println(new String(kv.getRow()) + "\t" 
//			 + new String(kv.getFamily()) + "\t" 
//			 + new String(kv.getQualifier()) + "\t" // get column
//			 +new String(kv.getValue()));
			}

		}
		System.out.println(c);
		
	}

	public static void main(String[] agrs) {
		try {
			String tablename = "img_info";
			// HbaseTestCase.creatTable(tablename);
//			 HbaseTestCase.addData(tablename);
			// HbaseTestCase.dropTable(tablename);
//			HbaseTestCase.getByRowKey(tablename, "1");
			HbaseTestCase.getAllData(tablename);
//			HbaseTestCase.deleteData(tablename, "0");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}