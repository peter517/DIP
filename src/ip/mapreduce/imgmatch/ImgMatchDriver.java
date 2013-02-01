package ip.mapreduce.imgmatch;

import main.DistriImgPro;
import model.GlobalVars;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;


public class ImgMatchDriver extends Configured implements Tool {

	public int run(String[] arg0) throws Exception {

		// set job
		Configuration conf = GlobalVars.getHbConf();

		Job job = new Job(conf, "img_match");
		job.setJarByClass(DistriImgPro.class);
		job.setSortComparatorClass(GlobalVars.TextDecreasingComparator.class);

		// set scan
		Scan scan = new Scan();
		// only scan sift
		scan.addColumn(GlobalVars.FAMILY_INFO.getBytes(), GlobalVars.COLUMN_SIFT_FEATURE.getBytes());
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
		scan.setCacheBlocks(false); // don't set to true for MR jobs

		TableMapReduceUtil.initTableMapperJob(GlobalVars.TABLE_IMG_INFO, scan, ImgMatchMapper.class, Text.class, Text.class, job);
		TableMapReduceUtil.initTableReducerJob(GlobalVars.TABLE_SIM_IMG, ImgMatchReducer.class, job);

		job.waitForCompletion(true);
		return 0;
	}

}
