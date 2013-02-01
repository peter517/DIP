package ip.mapreduce.sift2hbase;

import java.util.UUID;

import main.DistriImgPro;
import model.GlobalVars;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;


public class Sift2HbaseDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {

		Configuration conf = GlobalVars.getHbConf();
		Job job = new Job(conf, "extract_sift");
		job.setJarByClass(DistriImgPro.class);
		Path in = new Path(GlobalVars.SEQ_IMG_DIR);
		FileInputFormat.addInputPath(job, in);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(Sift2HbaseMapper.class);
		
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(NullOutputFormat.class);
		job.setMapOutputValueClass(NullOutputFormat.class);
		
		String uuid = UUID.randomUUID().toString();
		FileOutputFormat.setOutputPath(job, new Path("/tmp/mr/" + uuid));

		job.waitForCompletion(true);
		return 0;
	}

}
