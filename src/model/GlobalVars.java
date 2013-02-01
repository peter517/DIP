package model;

import ip.sift.Feature;

import java.io.File;
import java.io.IOException;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class GlobalVars {

	/** conf info **/
	// HDFS configuration
	private static Configuration conf = new Configuration();
	// HBase configuration
	private static Configuration hbConf = HBaseConfiguration.create();
	// control file operation
	private static FileSystem fs = null;
	
	//init the environment
	static {
		conf.set("fs.default.name", "hdfs://211.69.207.156:8021");
		hbConf.set("hbase.zookeeper.quorum", "211.69.207.156");
//		hbConf.set("io.bytes.per.checksum", "1");
		try { 
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/** fs info **/
	// root　dir
	public final static String ROOT_DIR = "hdfs://211.69.207.156:8021/user/root";
	// image upload dir
	public final static String QUERY_IMG_DIR = "queryImg";
	// image input dir
		public final static String INPUT_IMG_DIR = "imgInputDir";
	// the dir of sequence file of image
	public final static String SEQ_IMG_DIR = ROOT_DIR + File.separator + "imgOutputDir";
	// sequence file path
	public final static String SEQ_IMG_PATH = SEQ_IMG_DIR + File.separator + "seqFile";

	/** htable info **/
	// the separators of family
	public final static String SEPARATOR_ITEM = "\001";
	public final static String SEPARATOR_ARRAY = "\002";
	public final static String SEPARATOR_STRUCT = "\003";
	
	// img_info htable metadata
	public final static String TABLE_IMG_INFO = "img_info";
	public final static String FAMILY_INFO = "info";
	public final static String COLUMN_SIFT_FEATURE = "sift_feature";
	public final static String COLUMN_IMG_DATA = "img_data";
	
	// lsh_img htable metadata
	public final static String TABLE_LSH_IMG = "lsh_img";
	public final static String COLUMN_LSH_PATH = "lsh_path";
	
	// sim_img htable metadata
	public final static String TABLE_SIM_IMG = "sim_img";
	public final static String FAMILY_DST_IMG = "dst_img";
	public final static String COLUMN_SIM_PATH = "sim_path";

	/** other info **/
	// fVector of src img that need to be detected
	private static Vector<Feature> srcFVector = null;
	//src img path
	private static String srcPath = "";
	//the num of nearest neighbours 
	public static final int MOST_SIM_NUM = 10;
	
	public static ExecutorService pool = Executors.newFixedThreadPool(10);
	
	public static ExecutorService getPool() {
		return pool;
	}

	/**
	 * TextDecreasingComparator
	 */
	public static class TextDecreasingComparator extends Text.Comparator{  
		
        public int compare(WritableComparable  a, WritableComparable b) {  
          return -super.compare(a, b) ;
        }  
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {  
            return -super.compare(b1, s1, l1, b2, s2, l2);  
        }  
	}
	
	public static String getSrcPath() {
		return srcPath;
	}

	public static void setSrcPath(String srcPath) {
		GlobalVars.srcPath = srcPath;
	}

	public static Vector<Feature> getSrcFVector() {
		return srcFVector;
	}

	public static void setSrcFVector(Vector<Feature> srcFVector) {
		GlobalVars.srcFVector = srcFVector;
	}

	public static Configuration getConf() {
		return conf;
	}

	public static FileSystem getFs() {
		return fs;
	}

	public static Configuration getHbConf() {
		return hbConf;
	}



}
