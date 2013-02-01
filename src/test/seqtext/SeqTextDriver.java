package test.seqtext;

import model.GlobalVars;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;


public class SeqTextDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {

		Configuration conf = GlobalVars.getHbConf();

		Job job = new Job(conf, "ReadSeqFileToHbase");
		job.setJarByClass(Main.class);

		Path in = new Path("hdfs://localhost:8021/user/root/input01/");
		FileInputFormat.addInputPath(job, in);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		  
		job.setMapperClass(SeqTextMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		TableMapReduceUtil.initTableReducerJob("s4", SeqTextReducer.class, job);

		job.waitForCompletion(true);
		return 0;
	}

}
