package test.readhtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;

import tools.HbaseTableTools;

public class HtableDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {

		// set job
		Job job = new Job(HBaseConfiguration.create(), "ReadHbase");
		job.setJarByClass(Main.class);

		// set scan
		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
		scan.setCacheBlocks(false); // don't set to true for MR jobs

		TableMapReduceUtil.initTableMapperJob("tablename", scan, HtableMapper.class, Text.class, Text.class, job);
		TableMapReduceUtil.initTableReducerJob("tablename", HtableReducer.class, job);

		job.waitForCompletion(true);
		return 0;
	}

}
